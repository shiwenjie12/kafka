/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.log

import java.io._
import java.nio.ByteBuffer
import java.nio.file.Files

import kafka.common.KafkaException
import kafka.log.Log.offsetFromFile
import kafka.server.LogOffsetMetadata
import kafka.utils.{Logging, nonthreadsafe, threadsafe}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.protocol.types._
import org.apache.kafka.common.record.{ControlRecordType, EndTransactionMarker, RecordBatch}
import org.apache.kafka.common.utils.{ByteUtils, Crc32C}

import scala.collection.mutable.ListBuffer
import scala.collection.{immutable, mutable}

class CorruptSnapshotException(msg: String) extends KafkaException(msg)


// ValidationType及其子类型定义了在给定ProducerAppendInfo实例上执行验证的范围
private[log] sealed trait ValidationType
private[log] object ValidationType {

  /**
    * This indicates no validation should be performed on the incoming append. This is the case for all appends on
    * a replica, as well as appends when the producer state is being built from the log.
    */
  case object None extends ValidationType

  /**
    * We only validate the epoch (and not the sequence numbers) for offset commit requests coming from the transactional
    * producer. These appends will not have sequence numbers, so we can't validate them.
    */
  case object EpochOnly extends ValidationType

  /**
    * Perform the full validation. This should be used fo regular produce requests coming to the leader.
    */
  case object Full extends ValidationType
}

// 事务元数据
private[log] case class TxnMetadata(producerId: Long, var firstOffset: LogOffsetMetadata, var lastOffset: Option[Long] = None) {
  def this(producerId: Long, firstOffset: Long) = this(producerId, LogOffsetMetadata(firstOffset))

  override def toString: String = {
    "TxnMetadata(" +
      s"producerId=$producerId, " +
      s"firstOffset=$firstOffset, " +
      s"lastOffset=$lastOffset)"
  }
}

private[log] object ProducerIdEntry {
  private[log] val NumBatchesToRetain = 5
  def empty(producerId: Long) = new ProducerIdEntry(producerId, mutable.Queue[BatchMetadata](), RecordBatch.NO_PRODUCER_EPOCH, -1, None)
}

private[log] case class BatchMetadata(lastSeq: Int, lastOffset: Long, offsetDelta: Int, timestamp: Long) {
  def firstSeq = lastSeq - offsetDelta
  def firstOffset = lastOffset - offsetDelta

  override def toString: String = {
    "BatchMetadata(" +
      s"firstSeq=$firstSeq, " +
      s"lastSeq=$lastSeq, " +
      s"firstOffset=$firstOffset, " +
      s"lastOffset=$lastOffset, " +
      s"timestamp=$timestamp)"
  }
}

// batchMetadata是有序的，使得具有最低序列的批次位于队列的头上，而具有最高序列的批在队列的尾部。
// 我们将在队列中保留最多的ProducerIdEntry.NumBatchesToRetain元素。
// 当队列处于容量时，我们删除第一个元素来为传入的批处理创建空间。
private[log] class ProducerIdEntry(val producerId: Long, val batchMetadata: mutable.Queue[BatchMetadata],
                                   var producerEpoch: Short, var coordinatorEpoch: Int,
                                   var currentTxnFirstOffset: Option[Long]) {

  def firstSeq: Int = if (batchMetadata.isEmpty) RecordBatch.NO_SEQUENCE else batchMetadata.front.firstSeq
  def firstOffset: Long = if (batchMetadata.isEmpty) -1L else batchMetadata.front.firstOffset

  def lastSeq: Int = if (batchMetadata.isEmpty) RecordBatch.NO_SEQUENCE else batchMetadata.last.lastSeq
  def lastDataOffset: Long = if (batchMetadata.isEmpty) -1L else batchMetadata.last.lastOffset
  def lastTimestamp = if (batchMetadata.isEmpty) RecordBatch.NO_TIMESTAMP else batchMetadata.last.timestamp
  def lastOffsetDelta : Int = if (batchMetadata.isEmpty) 0 else batchMetadata.last.offsetDelta

  def addBatchMetadata(producerEpoch: Short, lastSeq: Int, lastOffset: Long, offsetDelta: Int, timestamp: Long) = {
    maybeUpdateEpoch(producerEpoch)

    if (batchMetadata.size == ProducerIdEntry.NumBatchesToRetain)
      batchMetadata.dequeue() // 出队

    batchMetadata.enqueue(BatchMetadata(lastSeq, lastOffset, offsetDelta, timestamp)) // 添加新的元数据
  }

  // 可能需要更新Epoch
  def maybeUpdateEpoch(producerEpoch: Short): Boolean = {
    if (this.producerEpoch != producerEpoch) {
      batchMetadata.clear()
      this.producerEpoch = producerEpoch
      true
    } else {
      false
    }
  }

  def removeBatchesOlderThan(offset: Long) = batchMetadata.dropWhile(_.lastOffset < offset)

  // 复制当前批次的元数据
  def duplicateOf(batch: RecordBatch): Option[BatchMetadata] = {
    if (batch.producerEpoch() != producerEpoch)
       None
    else
      batchWithSequenceRange(batch.baseSequence(), batch.lastSequence())
  }

  // 返回具有特定序列范围的缓存批处理的元数据（如果有的话）。
  def batchWithSequenceRange(firstSeq: Int, lastSeq: Int): Option[BatchMetadata] = {
    val duplicate = batchMetadata.filter { case(metadata) =>
      firstSeq == metadata.firstSeq && lastSeq == metadata.lastSeq
    }
    duplicate.headOption
  }

  override def toString: String = {
    "ProducerIdEntry(" +
      s"producerId=$producerId, " +
      s"producerEpoch=$producerEpoch, " +
      s"currentTxnFirstOffset=$currentTxnFirstOffset, " +
      s"coordinatorEpoch=$coordinatorEpoch, " +
      s"batchMetadata=$batchMetadata"
  }
}

/**
  * 该类用于验证给定生产者在写入日志之前附加的记录。
  * 它在上次成功追加后用生产者的状态进行初始化，并且可以对每个新记录的序列号和历元进行过渡性验证。
  * 另外，这个类在传入记录被验证时积累事务元数据。
 *
 * @param producerId The id of the producer appending to the log
 * @param currentEntry  The current entry associated with the producer id which contains metadata for a fixed number of
 *                      the most recent appends made by the producer. Validation of the first incoming append will
 *                      be made against the lastest append in the current entry. New appends will replace older appends
 *                      in the current entry so that the space overhead is constant.
 * @param validationType Indicates the extent of validation to perform on the appends on this instance. Offset commits
 *                       coming from the producer should have ValidationType.EpochOnly. Appends which aren't from a client
 *                       should have ValidationType.None. Appends coming from a client for produce requests should have
 *                       ValidationType.Full.
 */
private[log] class ProducerAppendInfo(val producerId: Long,
                                      currentEntry: ProducerIdEntry,
                                      validationType: ValidationType) {

  private val transactions = ListBuffer.empty[TxnMetadata]

  private def maybeValidateAppend(producerEpoch: Short, firstSeq: Int, lastSeq: Int) = {
    validationType match {
      case ValidationType.None =>

      case ValidationType.EpochOnly =>
        checkEpoch(producerEpoch)

      case ValidationType.Full =>
        checkEpoch(producerEpoch)
        checkSequence(producerEpoch, firstSeq, lastSeq)
    }
  }

  private def checkEpoch(producerEpoch: Short): Unit = {
    if (isFenced(producerEpoch)) {// 添加的小于当前的Epoch
      throw new ProducerFencedException(s"Producer's epoch is no longer valid. There is probably another producer " +
        s"with a newer epoch. $producerEpoch (request epoch), ${currentEntry.producerEpoch} (server epoch)")
    }
  }

  private def checkSequence(producerEpoch: Short, firstSeq: Int, lastSeq: Int): Unit = {
    if (producerEpoch != currentEntry.producerEpoch) {
      if (firstSeq != 0) {
        if (currentEntry.producerEpoch != RecordBatch.NO_PRODUCER_EPOCH) {
          throw new OutOfOrderSequenceException(s"Invalid sequence number for new epoch: $producerEpoch " +
            s"(request epoch), $firstSeq (seq. number)")
        } else {
          throw new UnknownProducerIdException(s"Found no record of producerId=$producerId on the broker. It is possible " +
            s"that the last message with the producerId=$producerId has been removed due to hitting the retention limit.")
        }
      }
    } else if (currentEntry.lastSeq == RecordBatch.NO_SEQUENCE && firstSeq != 0) {
      // the epoch was bumped by a control record, so we expect the sequence number to be reset
      throw new OutOfOrderSequenceException(s"Out of order sequence number for producerId $producerId: found $firstSeq " +
        s"(incoming seq. number), but expected 0")
    } else if (isDuplicate(firstSeq, lastSeq)) { // 幂等异常
      throw new DuplicateSequenceException(s"Duplicate sequence number for producerId $producerId: (incomingBatch.firstSeq, " +
        s"incomingBatch.lastSeq): ($firstSeq, $lastSeq).")
    } else if (!inSequence(firstSeq, lastSeq)) {
      throw new OutOfOrderSequenceException(s"Out of order sequence number for producerId $producerId: $firstSeq " +
        s"(incoming seq. number), ${currentEntry.lastSeq} (current end sequence number)")
    }
  }

  private def isDuplicate(firstSeq: Int, lastSeq: Int): Boolean = {
    ((lastSeq != 0 && currentEntry.firstSeq != Int.MaxValue && lastSeq < currentEntry.firstSeq)
      || currentEntry.batchWithSequenceRange(firstSeq, lastSeq).isDefined)
  }

  private def inSequence(firstSeq: Int, lastSeq: Int): Boolean = {
    firstSeq == currentEntry.lastSeq + 1L || (firstSeq == 0 && currentEntry.lastSeq == Int.MaxValue)
  }

  private def isFenced(producerEpoch: Short): Boolean = {
    producerEpoch < currentEntry.producerEpoch
  }

  // 向生产者状态管理器添加批次信息，分两种情况一种是普通的，一种是事务结束的
  def append(batch: RecordBatch): Option[CompletedTxn] = {
    if (batch.isControlBatch) {
      val record = batch.iterator.next()
      val endTxnMarker = EndTransactionMarker.deserialize(record) // 事务结束标记
      val completedTxn = appendEndTxnMarker(endTxnMarker, batch.producerEpoch, batch.baseOffset, record.timestamp)
      Some(completedTxn)
    } else {
      append(batch.producerEpoch, batch.baseSequence, batch.lastSequence, batch.maxTimestamp, batch.lastOffset,
        batch.isTransactional)
      None
    }
  }

  // 未开启控制批次的添加
  def append(epoch: Short,
             firstSeq: Int,
             lastSeq: Int,
             lastTimestamp: Long,
             lastOffset: Long,
             isTransactional: Boolean): Unit = {
    maybeValidateAppend(epoch, firstSeq, lastSeq)// 验证添加信息

    currentEntry.addBatchMetadata(epoch, lastSeq, lastOffset, lastSeq - firstSeq, lastTimestamp)// 添加批元数据

    if (currentEntry.currentTxnFirstOffset.isDefined && !isTransactional)
      throw new InvalidTxnStateException(s"Expected transactional write from producer $producerId")

    if (isTransactional && currentEntry.currentTxnFirstOffset.isEmpty) { // 为空则设置事务开启偏移量
      val firstOffset = lastOffset - (lastSeq - firstSeq)
      currentEntry.currentTxnFirstOffset = Some(firstOffset) // 当前事务的起始偏移量
      transactions += new TxnMetadata(producerId, firstOffset)
    }
  }

  // 添加结束事务的标记
  def appendEndTxnMarker(endTxnMarker: EndTransactionMarker,
                         producerEpoch: Short,
                         offset: Long,
                         timestamp: Long): CompletedTxn = {
    if (isFenced(producerEpoch))
      throw new ProducerFencedException(s"Invalid producer epoch: $producerEpoch (zombie): ${currentEntry.producerEpoch} (current)")

    if (currentEntry.coordinatorEpoch > endTxnMarker.coordinatorEpoch)
      throw new TransactionCoordinatorFencedException(s"Invalid coordinator epoch: ${endTxnMarker.coordinatorEpoch} " +
        s"(zombie), ${currentEntry.coordinatorEpoch} (current)")

    currentEntry.maybeUpdateEpoch(producerEpoch)

    val firstOffset = currentEntry.currentTxnFirstOffset match {
      case Some(txnFirstOffset) => txnFirstOffset // 本次事务的起始偏移量
      case None => // 本次消息的偏移量
        transactions += new TxnMetadata(producerId, offset)
        offset
    }

    currentEntry.currentTxnFirstOffset = None
    currentEntry.coordinatorEpoch = endTxnMarker.coordinatorEpoch
    CompletedTxn(producerId, firstOffset, offset, endTxnMarker.controlType == ControlRecordType.ABORT)
  }

  def latestEntry: ProducerIdEntry = currentEntry

  // 开启的事务
  def startedTransactions: List[TxnMetadata] = transactions.toList

  def maybeCacheTxnFirstOffsetMetadata(logOffsetMetadata: LogOffsetMetadata): Unit = {
    // we will cache the log offset metadata if it corresponds to the starting offset of
    // the last transaction that was started. This is optimized for leader appends where it
    // is only possible to have one transaction started for each log append, and the log
    // offset metadata will always match in that case since no data from other producers
    // is mixed into the append
    transactions.headOption.foreach { txn =>
      if (txn.firstOffset.messageOffset == logOffsetMetadata.messageOffset)
        txn.firstOffset = logOffsetMetadata
    }
  }

  override def toString: String = {
    "ProducerAppendInfo(" +
      s"producerId=$producerId, " +
      s"producerEpoch=${currentEntry.producerEpoch}, " +
      s"firstSequence=${currentEntry.firstSeq}, " +
      s"lastSequence=${currentEntry.lastSeq}, " +
      s"currentTxnFirstOffset=${currentEntry.currentTxnFirstOffset}, " +
      s"coordinatorEpoch=${currentEntry.coordinatorEpoch}, " +
      s"startedTransactions=$transactions)"
  }
}

object ProducerStateManager {
  private val ProducerSnapshotVersion: Short = 1
  private val VersionField = "version"
  private val CrcField = "crc"
  private val ProducerIdField = "producer_id"
  private val LastSequenceField = "last_sequence"
  private val ProducerEpochField = "epoch"
  private val LastOffsetField = "last_offset"
  private val OffsetDeltaField = "offset_delta"
  private val TimestampField = "timestamp"
  private val ProducerEntriesField = "producer_entries"
  private val CoordinatorEpochField = "coordinator_epoch"
  private val CurrentTxnFirstOffsetField = "current_txn_first_offset"

  private val VersionOffset = 0
  private val CrcOffset = VersionOffset + 2
  private val ProducerEntriesOffset = CrcOffset + 4

  val ProducerSnapshotEntrySchema = new Schema(
    new Field(ProducerIdField, Type.INT64, "The producer ID"),
    new Field(ProducerEpochField, Type.INT16, "Current epoch of the producer"),
    new Field(LastSequenceField, Type.INT32, "Last written sequence of the producer"),
    new Field(LastOffsetField, Type.INT64, "Last written offset of the producer"),
    new Field(OffsetDeltaField, Type.INT32, "The difference of the last sequence and first sequence in the last written batch"),
    new Field(TimestampField, Type.INT64, "Max timestamp from the last written entry"),
    new Field(CoordinatorEpochField, Type.INT32, "The epoch of the last transaction coordinator to send an end transaction marker"),
    new Field(CurrentTxnFirstOffsetField, Type.INT64, "The first offset of the on-going transaction (-1 if there is none)"))
  val PidSnapshotMapSchema = new Schema(
    new Field(VersionField, Type.INT16, "Version of the snapshot file"),
    new Field(CrcField, Type.UNSIGNED_INT32, "CRC of the snapshot data"),
    new Field(ProducerEntriesField, new ArrayOf(ProducerSnapshotEntrySchema), "The entries in the producer table"))

  // 读取快照 读取ProducerIdEntry
  def readSnapshot(file: File): Iterable[ProducerIdEntry] = {
    try {
      val buffer = Files.readAllBytes(file.toPath)
      val struct = PidSnapshotMapSchema.read(ByteBuffer.wrap(buffer))

      val version = struct.getShort(VersionField)
      if (version != ProducerSnapshotVersion)
        throw new CorruptSnapshotException(s"Snapshot contained an unknown file version $version")

      val crc = struct.getUnsignedInt(CrcField)
      val computedCrc =  Crc32C.compute(buffer, ProducerEntriesOffset, buffer.length - ProducerEntriesOffset)
      if (crc != computedCrc)
        throw new CorruptSnapshotException(s"Snapshot is corrupt (CRC is no longer valid). " +
          s"Stored crc: $crc. Computed crc: $computedCrc")

      struct.getArray(ProducerEntriesField).map { producerEntryObj =>
        val producerEntryStruct = producerEntryObj.asInstanceOf[Struct]
        val producerId: Long = producerEntryStruct.getLong(ProducerIdField)
        val producerEpoch = producerEntryStruct.getShort(ProducerEpochField)
        val seq = producerEntryStruct.getInt(LastSequenceField)
        val offset = producerEntryStruct.getLong(LastOffsetField)
        val timestamp = producerEntryStruct.getLong(TimestampField)
        val offsetDelta = producerEntryStruct.getInt(OffsetDeltaField)
        val coordinatorEpoch = producerEntryStruct.getInt(CoordinatorEpochField)
        val currentTxnFirstOffset = producerEntryStruct.getLong(CurrentTxnFirstOffsetField)
        val newEntry = new ProducerIdEntry(producerId, mutable.Queue[BatchMetadata](BatchMetadata(seq, offset, offsetDelta, timestamp)), producerEpoch,
          coordinatorEpoch, if (currentTxnFirstOffset >= 0) Some(currentTxnFirstOffset) else None)
        newEntry
      }
    } catch {
      case e: SchemaException =>
        throw new CorruptSnapshotException(s"Snapshot failed schema validation: ${e.getMessage}")
    }
  }

  private def writeSnapshot(file: File, entries: mutable.Map[Long, ProducerIdEntry]) {
    val struct = new Struct(PidSnapshotMapSchema)
    struct.set(VersionField, ProducerSnapshotVersion)
    struct.set(CrcField, 0L) // we'll fill this after writing the entries
    val entriesArray = entries.map {
      case (producerId, entry) =>
        val producerEntryStruct = struct.instance(ProducerEntriesField)
        producerEntryStruct.set(ProducerIdField, producerId)
          .set(ProducerEpochField, entry.producerEpoch)
          .set(LastSequenceField, entry.lastSeq)
          .set(LastOffsetField, entry.lastDataOffset)
          .set(OffsetDeltaField, entry.lastOffsetDelta)
          .set(TimestampField, entry.lastTimestamp)
          .set(CoordinatorEpochField, entry.coordinatorEpoch)
          .set(CurrentTxnFirstOffsetField, entry.currentTxnFirstOffset.getOrElse(-1L))
        producerEntryStruct
    }.toArray
    struct.set(ProducerEntriesField, entriesArray)

    val buffer = ByteBuffer.allocate(struct.sizeOf)
    struct.writeTo(buffer)
    buffer.flip()

    // now fill in the CRC
    val crc = Crc32C.compute(buffer, ProducerEntriesOffset, buffer.limit() - ProducerEntriesOffset)
    ByteUtils.writeUnsignedInt(buffer, CrcOffset, crc)

    val fos = new FileOutputStream(file)
    try {
      fos.write(buffer.array, buffer.arrayOffset, buffer.limit())
    } finally {
      fos.close()
    }
  }

  private def isSnapshotFile(file: File): Boolean = file.getName.endsWith(Log.ProducerSnapshotFileSuffix)

  // 文件目录下的生产者快照文件
  private[log] def listSnapshotFiles(dir: File): Seq[File] = {
    if (dir.exists && dir.isDirectory) {
      Option(dir.listFiles).map { files =>
        files.filter(f => f.isFile && isSnapshotFile(f)).toSeq
      }.getOrElse(Seq.empty)
    } else Seq.empty
  }

  // visible for testing
  private[log] def deleteSnapshotsBefore(dir: File, offset: Long): Unit = deleteSnapshotFiles(dir, _ < offset)

  // 删除符合条件的快照文件
  private def deleteSnapshotFiles(dir: File, predicate: Long => Boolean = _ => true) {
    listSnapshotFiles(dir).filter(file => predicate(offsetFromFile(file))).foreach { file =>
      Files.deleteIfExists(file.toPath)
    }
  }

}

/**
  * 维护从产品ID到元数据的最后一个附加条目（例如，历元、序列号、最后偏移等）的映射。
 *
 * The sequence number is the last number successfully appended to the partition for the given identifier.
 * The epoch is used for fencing against zombie writers. The offset is the one of the last successful message
 * appended to the partition.
 *
 * As long as a producer id is contained in the map, the corresponding producer can continue to write data.
 * However, producer ids can be expired due to lack of recent use or if the last written entry has been deleted from
 * the log (e.g. if the retention policy is "delete"). For compacted topics, the log cleaner will ensure
 * that the most recent entry from a given producer id is retained in the log provided it hasn't expired due to
 * age. This ensures that producer ids will not be expired until either the max expiration time has been reached,
 * or if the topic also is configured for deletion, the segment containing the last written offset has
 * been deleted.
 */
@nonthreadsafe
class ProducerStateManager(val topicPartition: TopicPartition,
                           @volatile var logDir: File,
                           val maxProducerIdExpirationMs: Int = 60 * 60 * 1000) extends Logging {
  import ProducerStateManager._
  import java.util

  private val producers = mutable.Map.empty[Long, ProducerIdEntry]
  private var lastMapOffset = 0L
  private var lastSnapOffset = 0L

  // 正在进行的交易按交易的第一笔抵消进行排序
  private val ongoingTxns = new util.TreeMap[Long, TxnMetadata]

  // 已完成的交易，其标记位于高水印以上偏移处
  private val unreplicatedTxns = new util.TreeMap[Long, TxnMetadata]

  /**
    * 不稳定偏移是一个未决定的（即其最终结果尚不知道），
    * 或者是一个被确定但可能未被复制的（即，具有比当前高水印写入更高偏移量的提交/中止标记的任何事务）。
   */
  def firstUnstableOffset: Option[LogOffsetMetadata] = {
    val unreplicatedFirstOffset = Option(unreplicatedTxns.firstEntry).map(_.getValue.firstOffset) // 未复制的
    val undecidedFirstOffset = Option(ongoingTxns.firstEntry).map(_.getValue.firstOffset)//  未确定的
    if (unreplicatedFirstOffset.isEmpty)
      undecidedFirstOffset
    else if (undecidedFirstOffset.isEmpty)
      unreplicatedFirstOffset
    else if (undecidedFirstOffset.get.messageOffset < unreplicatedFirstOffset.get.messageOffset)
      undecidedFirstOffset
    else
      unreplicatedFirstOffset
  }

  /**
    * 确认在给定偏移之前完成的所有事务。这允许LSO前进到下一个不稳定偏移。
   */
  def onHighWatermarkUpdated(highWatermark: Long): Unit = {
    removeUnreplicatedTransactions(highWatermark)
  }

  /**
   * 第一个未定的偏移量    是尚未提交或中止的最早的事务消息。
   */
  def firstUndecidedOffset: Option[Long] = Option(ongoingTxns.firstEntry).map(_.getValue.firstOffset.messageOffset)

  /**
   * Returns the last offset of this map
   */
  def mapEndOffset = lastMapOffset

  /**
   * Get a copy of the active producers
   */
  def activeProducers: immutable.Map[Long, ProducerIdEntry] = producers.toMap

  def isEmpty: Boolean = producers.isEmpty && unreplicatedTxns.isEmpty

  // 从快照中加载生产者状态数据
  private def loadFromSnapshot(logStartOffset: Long, currentTime: Long) {
    while (true) {
      latestSnapshotFile match {
        case Some(file) =>
          try {
            info(s"Loading producer state from snapshot file '$file' for partition $topicPartition")
            val loadedProducers = readSnapshot(file).filter { producerEntry =>
              isProducerRetained(producerEntry, logStartOffset) && !isProducerExpired(currentTime, producerEntry)
            }
            loadedProducers.foreach(loadProducerEntry)
            lastSnapOffset = offsetFromFile(file)
            lastMapOffset = lastSnapOffset
            return
          } catch {
            case e: CorruptSnapshotException =>
              warn(s"Failed to load producer snapshot from '$file': ${e.getMessage}")
              Files.deleteIfExists(file.toPath)
          }
        case None =>
          lastSnapOffset = logStartOffset
          lastMapOffset = logStartOffset
          return
      }
    }
  }

  // 将ProducerIdEntry加载producers ongoingTxns
  private[log] def loadProducerEntry(entry: ProducerIdEntry): Unit = {
    val producerId = entry.producerId
    producers.put(producerId, entry)
    entry.currentTxnFirstOffset.foreach { offset =>
      ongoingTxns.put(offset, new TxnMetadata(producerId, offset))
    }
  }

  private def isProducerExpired(currentTimeMs: Long, producerIdEntry: ProducerIdEntry): Boolean =
    producerIdEntry.currentTxnFirstOffset.isEmpty && currentTimeMs - producerIdEntry.lastTimestamp >= maxProducerIdExpirationMs

  /**
   * Expire any producer ids which have been idle longer than the configured maximum expiration timeout.
   */
  def removeExpiredProducers(currentTimeMs: Long) {
    producers.retain { case (producerId, lastEntry) =>
      !isProducerExpired(currentTimeMs, lastEntry)
    }
  }

  /**
    * 将生产者标识映射截断到给定的偏移范围，并重新加载范围中最近快照的条目（如果有的话）。
    * 请注意，日志结束偏移被假定为小于或等于高水印。
    * @param logStartOffset tp的起始偏移量
    * @param logEndOffset 之前正常的偏移量
   */
  def truncateAndReload(logStartOffset: Long, logEndOffset: Long, currentTimeMs: Long) {
    // 删除所有超出范围的快照
    deleteSnapshotFiles(logDir, { snapOffset =>
      snapOffset > logEndOffset || snapOffset <= logStartOffset
    })

    if (logEndOffset != mapEndOffset) {
      producers.clear()
      ongoingTxns.clear()

      // since we assume that the offset is less than or equal to the high watermark, it is
      // safe to clear the unreplicated transactions
      unreplicatedTxns.clear()
      loadFromSnapshot(logStartOffset, currentTimeMs)
    } else {
      truncateHead(logStartOffset)
    }
  }

  // 产生生产者级别的
  def prepareUpdate(producerId: Long, isFromClient: Boolean): ProducerAppendInfo = {
    val validationToPerform = // 用于执行验证的类型
      if (!isFromClient)
        ValidationType.None
      else if (topicPartition.topic == Topic.GROUP_METADATA_TOPIC_NAME)
        ValidationType.EpochOnly
      else
        ValidationType.Full

    new ProducerAppendInfo(producerId, lastEntry(producerId).getOrElse(ProducerIdEntry.empty(producerId)), validationToPerform)
  }

  /**
   * 用给定的附加信息更新映射（producers、ongoingTxns）
   */
  def update(appendInfo: ProducerAppendInfo): Unit = {
    if (appendInfo.producerId == RecordBatch.NO_PRODUCER_ID)
      throw new IllegalArgumentException(s"Invalid producer id ${appendInfo.producerId} passed to update")

    trace(s"Updated producer ${appendInfo.producerId} state to $appendInfo")

    val entry = appendInfo.latestEntry
    producers.put(appendInfo.producerId, entry)
    appendInfo.startedTransactions.foreach { txn =>
      ongoingTxns.put(txn.firstOffset.messageOffset, txn)
    }
  }

  def updateMapEndOffset(lastOffset: Long): Unit = {
    lastMapOffset = lastOffset
  }

  /**
   * 获取给定生产者ID的最后写入entry
   */
  def lastEntry(producerId: Long): Option[ProducerIdEntry] = producers.get(producerId)

  /**
   * 如果尚不存在，则在当前末端偏移处拍摄快照。
   */
  def takeSnapshot(): Unit = {
    // 如果是新的偏移量，那么拍一张快照
    if (lastMapOffset > lastSnapOffset) {
      val snapshotFile = Log.producerSnapshotFile(logDir, lastMapOffset)
      debug(s"Writing producer snapshot for partition $topicPartition at offset $lastMapOffset")
      writeSnapshot(snapshotFile, producers)// 写入生成的快照

      // Update the last snap offset according to the serialized map
      lastSnapOffset = lastMapOffset
    }
  }

  /**
   * Get the last offset (exclusive) of the latest snapshot file.
   */
  def latestSnapshotOffset: Option[Long] = latestSnapshotFile.map(file => offsetFromFile(file))

  /**
   * Get the last offset (exclusive) of the oldest snapshot file.
   */
  def oldestSnapshotOffset: Option[Long] = oldestSnapshotFile.map(file => offsetFromFile(file))

  private def isProducerRetained(producerIdEntry: ProducerIdEntry, logStartOffset: Long): Boolean = {
    producerIdEntry.removeBatchesOlderThan(logStartOffset)
    producerIdEntry.lastDataOffset >= logStartOffset
  }

  /**
   * When we remove the head of the log due to retention, we need to clean up the id map. This method takes
   * the new start offset and removes all producerIds which have a smaller last written offset. Additionally,
   * we remove snapshots older than the new log start offset.
   *
   * Note that snapshots from offsets greater than the log start offset may have producers included which
   * should no longer be retained: these producers will be removed if and when we need to load state from
   * the snapshot.
   */
  def truncateHead(logStartOffset: Long) {
    val evictedProducerEntries = producers.filter { case (_, producerIdEntry) =>
      !isProducerRetained(producerIdEntry, logStartOffset)
    }
    val evictedProducerIds = evictedProducerEntries.keySet

    producers --= evictedProducerIds
    removeEvictedOngoingTransactions(evictedProducerIds)
    removeUnreplicatedTransactions(logStartOffset)

    if (lastMapOffset < logStartOffset)
      lastMapOffset = logStartOffset

    deleteSnapshotsBefore(logStartOffset)
    lastSnapOffset = latestSnapshotOffset.getOrElse(logStartOffset)
  }

  private def removeEvictedOngoingTransactions(expiredProducerIds: collection.Set[Long]): Unit = {
    val iterator = ongoingTxns.entrySet.iterator
    while (iterator.hasNext) {
      val txnEntry = iterator.next()
      if (expiredProducerIds.contains(txnEntry.getValue.producerId))
        iterator.remove()
    }
  }

  private def removeUnreplicatedTransactions(offset: Long): Unit = {
    val iterator = unreplicatedTxns.entrySet.iterator
    while (iterator.hasNext) {
      val txnEntry = iterator.next()
      val lastOffset = txnEntry.getValue.lastOffset
      if (lastOffset.exists(_ < offset))
        iterator.remove()
    }
  }

  /**
   * Truncate the producer id mapping and remove all snapshots. This resets the state of the mapping.
   */
  def truncate() {
    producers.clear()
    ongoingTxns.clear()
    unreplicatedTxns.clear()
    deleteSnapshotFiles(logDir)
    lastSnapOffset = 0L
    lastMapOffset = 0L
  }

  /**
    * 完成事务并返回最后稳定的偏移量
   */
  def completeTxn(completedTxn: CompletedTxn): Long = {
    val txnMetadata = ongoingTxns.remove(completedTxn.firstOffset) // 从正在进行的事务中移除完成事务
    if (txnMetadata == null)
      throw new IllegalArgumentException("Attempted to complete a transaction which was not started")

    // 标记事务元素的完成偏移量
    txnMetadata.lastOffset = Some(completedTxn.lastOffset)
    unreplicatedTxns.put(completedTxn.firstOffset, txnMetadata)

    val lastStableOffset = firstUndecidedOffset.getOrElse(completedTxn.lastOffset + 1)
    lastStableOffset
  }

  @threadsafe
  def deleteSnapshotsBefore(offset: Long): Unit = ProducerStateManager.deleteSnapshotsBefore(logDir, offset)

  private def oldestSnapshotFile: Option[File] = {
    val files = listSnapshotFiles
    if (files.nonEmpty)
      Some(files.minBy(offsetFromFile))
    else
      None
  }

  // 获取最新的快照文件
  private def latestSnapshotFile: Option[File] = {
    val files = listSnapshotFiles
    if (files.nonEmpty)
      Some(files.maxBy(offsetFromFile))
    else
      None
  }

  // 所有快照文件
  private def listSnapshotFiles: Seq[File] = ProducerStateManager.listSnapshotFiles(logDir)

}
