/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.io.{File, IOException}
import java.nio.file.{Files, NoSuchFileException}
import java.nio.file.attribute.FileTime
import java.util.concurrent.TimeUnit

import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.server.epoch.LeaderEpochCache
import kafka.server.{FetchDataInfo, LogOffsetMetadata}
import kafka.utils._
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.record.FileRecords.LogOffsetPosition
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.Time

import scala.collection.JavaConverters._
import scala.math._

/**
 * A segment of the log. Each segment has two components: a log and an index. The log is a FileMessageSet containing
 * the actual messages. The index is an OffsetIndex that maps from logical offsets to physical file positions. Each
 * segment has a base offset which is an offset <= the least offset of any message in this segment and > any offset in
 * any previous segment.
  * 日志的一部分。每个段都有两个组件：日志和索引。
  * 日志是包含实际消息的FielMessageSet(已移除)。该索引是从逻辑偏移映射到物理文件位置的OffStand索引。
  * 每个段都有一个基础偏移，它是一个偏移量<=这个段中的任何消息的最小偏移量和任何先前段中的任何偏移量。
 *
 * A segment with a base offset of [base_offset] would be stored in two files, a [base_offset].index and a [base_offset].log file.
 *
 * @param log The message set containing log entries
 * @param offsetIndex The offset index
 * @param timeIndex The timestamp index
 * @param baseOffset A lower bound on the offsets in this segment
 * @param indexIntervalBytes The approximate number of bytes between entries in the index
 * @param time The time instance
 */
@nonthreadsafe
class LogSegment private[log] (val log: FileRecords,
                               val offsetIndex: OffsetIndex,
                               val timeIndex: TimeIndex,
                               val txnIndex: TransactionIndex,
                               val baseOffset: Long,
                               val indexIntervalBytes: Int,
                               val rollJitterMs: Long,
                               val maxSegmentMs: Long,
                               val maxSegmentBytes: Int,
                               val time: Time) extends Logging {

  // 判断是否应该回滚
  def shouldRoll(messagesSize: Int, maxTimestampInMessages: Long, maxOffsetInMessages: Long, now: Long): Boolean = {
    val reachedRollMs = timeWaitedForRoll(now, maxTimestampInMessages) > maxSegmentMs - rollJitterMs
    size > maxSegmentBytes - messagesSize ||
      (size > 0 && reachedRollMs) ||
      offsetIndex.isFull || timeIndex.isFull || !canConvertToRelativeOffset(maxOffsetInMessages)
  }

  def resizeIndexes(size: Int): Unit = {
    offsetIndex.resize(size)
    timeIndex.resize(size)
  }

  // 可用性检查
  def sanityCheck(timeIndexFileNewlyCreated: Boolean): Unit = {
    if (offsetIndex.file.exists) {
      offsetIndex.sanityCheck()
      // Resize the time index file to 0 if it is newly created.
      if (timeIndexFileNewlyCreated)
        timeIndex.resize(0)
      timeIndex.sanityCheck()
      txnIndex.sanityCheck()
    }
    else throw new NoSuchFileException(s"Offset index file ${offsetIndex.file.getAbsolutePath} does not exist")
  }

  private var created = time.milliseconds

  /* 自上次在偏移量索引中添加条目以来的字节数 */
  private var bytesSinceLastIndexEntry = 0

  /* 用于时间日志滚动的时间戳 */
  private var rollingBasedTimestamp: Option[Long] = None

  /* The maximum timestamp we see so far */
  @volatile private var maxTimestampSoFar: Long = timeIndex.lastEntry.timestamp
  @volatile private var offsetOfMaxTimestamp: Long = timeIndex.lastEntry.offset

  /* Return the size in bytes of this log segment */
  def size: Int = log.sizeInBytes()

  /**
   * 检查参数偏移可以表示为相对于基数偏移量的整数偏移量。
   */
  def canConvertToRelativeOffset(offset: Long): Boolean = {
    (offset - baseOffset) <= Integer.MAX_VALUE
  }

  /**
    * 从给定的偏移量开始追加给定的消息。如果需要，向索引中添加条目。
   *
   * 假设此方法是从锁内调用的。
   *
   * @param firstOffset 消息集中的第一个偏移量。
   * @param largestOffset 消息集中的最后一个偏移量
   * @param largestTimestamp 消息集中最大的时间戳。
   * @param shallowOffsetOfMaxTimestamp 在要附加的消息中具有最大时间戳的消息的偏移量。
   * @param records The log entries to append.
   * @return the physical position in the file of the appended records
   */
  @nonthreadsafe
  def append(firstOffset: Long,
             largestOffset: Long,
             largestTimestamp: Long,
             shallowOffsetOfMaxTimestamp: Long,
             records: MemoryRecords): Unit = {
    if (records.sizeInBytes > 0) {
      trace("Inserting %d bytes at offset %d at position %d with largest timestamp %d at shallow offset %d"
          .format(records.sizeInBytes, firstOffset, log.sizeInBytes(), largestTimestamp, shallowOffsetOfMaxTimestamp))
      val physicalPosition = log.sizeInBytes()// log中的大小，即已占有的大小
      if (physicalPosition == 0)
        rollingBasedTimestamp = Some(largestTimestamp)
      // 添加消息
      require(canConvertToRelativeOffset(largestOffset), "largest offset in message set can not be safely converted to relative offset.")
      val appendedBytes = log.append(records)
      trace(s"Appended $appendedBytes to ${log.file()} at offset $firstOffset")
      // 更新内存最大时间戳和相应的偏移量。
      if (largestTimestamp > maxTimestampSoFar) {
        maxTimestampSoFar = largestTimestamp
        offsetOfMaxTimestamp = shallowOffsetOfMaxTimestamp
      }
      // 如果需要将实体添加索引中
      if(bytesSinceLastIndexEntry > indexIntervalBytes) {
        offsetIndex.append(firstOffset, physicalPosition)
        timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestamp)
        bytesSinceLastIndexEntry = 0
      }
      bytesSinceLastIndexEntry += records.sizeInBytes
    }
  }

  @nonthreadsafe
  def updateTxnIndex(completedTxn: CompletedTxn, lastStableOffset: Long) {
    if (completedTxn.isAborted) { // 中断才添加索引
      trace(s"Writing aborted transaction $completedTxn to transaction index, last stable offset is $lastStableOffset")
      txnIndex.append(new AbortedTxn(completedTxn, lastStableOffset))
    }
  }

  private def updateProducerState(producerStateManager: ProducerStateManager, batch: RecordBatch): Unit = {
    if (batch.hasProducerId) {
      val producerId = batch.producerId
      val appendInfo = producerStateManager.prepareUpdate(producerId, isFromClient = false)// 获取添加信息
      val maybeCompletedTxn = appendInfo.append(batch)
      producerStateManager.update(appendInfo)
      maybeCompletedTxn.foreach { completedTxn =>
        val lastStableOffset = producerStateManager.completeTxn(completedTxn)
        updateTxnIndex(completedTxn, lastStableOffset)
      }
    }
    producerStateManager.updateMapEndOffset(batch.lastOffset + 1)
  }

  /**
   * Find the physical file position for the first message with offset >= the requested offset.
   * 从offset索引中解析LogOffsetPosition
    * 先在索引文件中进行二分法查找，然后在利用获取的索引信息的起始偏移量在，日志文件中查找
   * The startingFilePosition argument is an optimization that can be used if we already know a valid starting position
   * in the file higher than the greatest-lower-bound from the index.
   *
   * @param offset The offset we want to translate
   * @param startingFilePosition A lower bound on the file position from which to begin the search. This is purely an optimization and
   * when omitted, the search will begin at the position in the offset index.
   * @return The position in the log storing the message with the least offset >= the requested offset and the size of the
    *        message or null if no message meets this criteria.
   */
  @threadsafe
  private[log] def translateOffset(offset: Long, startingFilePosition: Int = 0): LogOffsetPosition = {
    val mapping = offsetIndex.lookup(offset)// 索引实体
    log.searchForOffsetWithSize(offset, max(mapping.position, startingFilePosition))// 日志实体
  }

  /**
    * 从第一个偏移量> = startOffset开始，从这个段读取消息集。
    * 消息集将包含不超过maxSize字节，并且如果指定了maxOffset，它将在maxOffset之前结束。
   * @param startOffset 包含在我们阅读的消息集中的第一个偏移量的下标
   * @param maxSize 我们读取的消息集中包含的最大字节数
   * @param maxOffset 我们阅读的消息集的可选最大偏移量
   * @param maxPosition 日志段中应该暴露以供读取的最大位置
   * @param minOneMessage If this is true, the first message will be returned even if it exceeds `maxSize` (if one exists)
   *
   * @return The fetched data and the offset metadata of the first message whose offset is >= startOffset,
   *         or null if the startOffset is larger than the largest offset in this log
   */
  @threadsafe
  def read(startOffset: Long, maxOffset: Option[Long], maxSize: Int, maxPosition: Long = size,
           minOneMessage: Boolean = false): FetchDataInfo = {
    if (maxSize < 0)
      throw new IllegalArgumentException("Invalid max size for log read (%d)".format(maxSize))

    val logSize = log.sizeInBytes // this may change, need to save a consistent copy
    val startOffsetAndSize = translateOffset(startOffset)

    // 如果起始位置已经离开日志的末尾，则返回null
    if (startOffsetAndSize == null)
      return null

    val startPosition = startOffsetAndSize.position
    val offsetMetadata = new LogOffsetMetadata(startOffset, this.baseOffset, startPosition)

    val adjustedMaxSize =
      if (minOneMessage) math.max(maxSize, startOffsetAndSize.size)
      else maxSize

    // return a log segment but with zero size in the case below
    if (adjustedMaxSize == 0)
      return FetchDataInfo(offsetMetadata, MemoryRecords.EMPTY)

    // 根据是否给我们一个最大偏移量来计算要读取的消息集的长度。
    val fetchSize: Int = maxOffset match {
      case None =>
        // no max offset, just read until the max position
        min((maxPosition - startPosition).toInt, adjustedMaxSize)
      case Some(offset) =>
        // there is a max offset, translate it to a file position and use that to calculate the max read size;
        // when the leader of a partition changes, it's possible for the new leader's high watermark to be less than the
        // true high watermark in the previous leader for a short window. In this window, if a consumer fetches on an
        // offset between new leader's high watermark and the log end offset, we want to return an empty response.
        if (offset < startOffset)
          return FetchDataInfo(offsetMetadata, MemoryRecords.EMPTY, firstEntryIncomplete = false)
        val mapping = translateOffset(offset, startPosition)
        val endPosition =
          if (mapping == null)
            logSize // the max offset is off the end of the log, use the end of the file
          else
            mapping.position
        min(min(maxPosition, endPosition) - startPosition, adjustedMaxSize).toInt
    }

    FetchDataInfo(offsetMetadata, log.read(startPosition, fetchSize),
      firstEntryIncomplete = adjustedMaxSize < startOffsetAndSize.size)
  }

   def fetchUpperBoundOffset(startOffsetPosition: OffsetPosition, fetchSize: Int): Option[Long] =
     offsetIndex.fetchUpperBoundOffset(startOffsetPosition, fetchSize).map(_.offset)

  /**
    * 在给定的段上运行恢复。这将从日志文件中重建索引，并从日志和索引的末尾删除任何无效字节。
   *
   * @param producerStateManager 对应于段基偏移的生产者状态。这是恢复事务索引所必需的。
   * @param leaderEpochCache 可选地，用于在恢复期间更新领导者时期的高速缓存。
   * @return The number of bytes truncated from the log
   */
  @nonthreadsafe
  def recover(producerStateManager: ProducerStateManager, leaderEpochCache: Option[LeaderEpochCache] = None): Int = {
    // 只对索引进行重建
    offsetIndex.reset()
    timeIndex.reset()
    txnIndex.reset()
    var validBytes = 0
    var lastIndexEntry = 0
    maxTimestampSoFar = RecordBatch.NO_TIMESTAMP
    try {
      for (batch <- log.batches.asScala) {
        batch.ensureValid()// 确保是有效的

        // The max timestamp is exposed at the batch level, so no need to iterate the records
        if (batch.maxTimestamp > maxTimestampSoFar) {
          maxTimestampSoFar = batch.maxTimestamp
          offsetOfMaxTimestamp = batch.lastOffset
        }

        // 构建offset索引
        if (validBytes - lastIndexEntry > indexIntervalBytes) {
          val startOffset = batch.baseOffset
          offsetIndex.append(startOffset, validBytes)
          timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestamp)
          lastIndexEntry = validBytes
        }
        validBytes += batch.sizeInBytes()

        if (batch.magic >= RecordBatch.MAGIC_VALUE_V2) {// 只有V2才有 LeaderEpoch
          leaderEpochCache.foreach { cache =>
            if (batch.partitionLeaderEpoch > cache.latestEpoch()) // this is to avoid unnecessary warning in cache.assign()
              cache.assign(batch.partitionLeaderEpoch, batch.baseOffset)// 向缓存中添加epoch
          }
          updateProducerState(producerStateManager, batch)// 事务管理，之后在看吧
        }
      }
    } catch {
      case e: CorruptRecordException =>
        warn("Found invalid messages in log segment %s at byte offset %d: %s."
          .format(log.file.getAbsolutePath, validBytes, e.getMessage))
    }
    val truncated = log.sizeInBytes - validBytes  // 截取的无效字节
    if (truncated > 0)
      debug(s"Truncated $truncated invalid bytes at the end of segment ${log.file.getAbsoluteFile} during recovery")

    log.truncateTo(validBytes)
    offsetIndex.trimToValidSize()
    // A normally closed segment always appends the biggest timestamp ever seen into log segment, we do this as well.
    timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestamp, skipFullCheck = true)
    timeIndex.trimToValidSize()
    truncated
  }

  private def loadLargestTimestamp() {
    // Get the last time index entry. If the time index is empty, it will return (-1, baseOffset)
    val lastTimeIndexEntry = timeIndex.lastEntry
    maxTimestampSoFar = lastTimeIndexEntry.timestamp
    offsetOfMaxTimestamp = lastTimeIndexEntry.offset

    val offsetPosition = offsetIndex.lookup(lastTimeIndexEntry.offset)
    // Scan the rest of the messages to see if there is a larger timestamp after the last time index entry.
    val maxTimestampOffsetAfterLastEntry = log.largestTimestampAfter(offsetPosition.position)
    if (maxTimestampOffsetAfterLastEntry.timestamp > lastTimeIndexEntry.timestamp) {
      maxTimestampSoFar = maxTimestampOffsetAfterLastEntry.timestamp
      offsetOfMaxTimestamp = maxTimestampOffsetAfterLastEntry.offset
    }
  }

  def collectAbortedTxns(fetchOffset: Long, upperBoundOffset: Long): TxnIndexSearchResult =
    txnIndex.collectAbortedTxns(fetchOffset, upperBoundOffset)

  override def toString = "LogSegment(baseOffset=" + baseOffset + ", size=" + size + ")"

  /**
   * Truncate off all index and log entries with offsets >= the given offset.
   * If the given offset is larger than the largest message in this segment, do nothing.
   *
   * @param offset The offset to truncate to
   * @return The number of log bytes truncated
   */
  @nonthreadsafe
  def truncateTo(offset: Long): Int = {
    // Do offset translation before truncating the index to avoid needless scanning
    // in case we truncate the full index
    val mapping = translateOffset(offset)
    offsetIndex.truncateTo(offset)
    timeIndex.truncateTo(offset)
    txnIndex.truncateTo(offset)

    // After truncation, reset and allocate more space for the (new currently active) index
    offsetIndex.resize(offsetIndex.maxIndexSize)
    timeIndex.resize(timeIndex.maxIndexSize)

    val bytesTruncated = if (mapping == null) 0 else log.truncateTo(mapping.position)
    if (log.sizeInBytes == 0) {
      created = time.milliseconds
      rollingBasedTimestamp = None
    }

    bytesSinceLastIndexEntry = 0
    if (maxTimestampSoFar >= 0)
      loadLargestTimestamp()
    bytesTruncated
  }

  /**
   * 计算将被用于下一条要附加到该段的消息的偏移量。 请注意，这是昂贵的。
   */
  @threadsafe
  def readNextOffset: Long = {
    val ms = read(offsetIndex.lastOffset, None, log.sizeInBytes)
    if (ms == null)
      baseOffset
    else
      ms.records.batches.asScala.lastOption
        .map(_.nextOffset)
        .getOrElse(baseOffset)
  }

  /**
   * Flush this log segment to disk
   */
  @threadsafe
  def flush() {
    LogFlushStats.logFlushTimer.time {
      log.flush()
      offsetIndex.flush()
      timeIndex.flush()
      txnIndex.flush()
    }
  }

  /**
   * Update the directory reference for the log and indices in this segment. This would typically be called after a
   * directory is renamed.
   */
  def updateDir(dir: File): Unit = {
    log.setFile(new File(dir, log.file.getName))
    offsetIndex.file = new File(dir, offsetIndex.file.getName)
    timeIndex.file = new File(dir, timeIndex.file.getName)
    txnIndex.file = new File(dir, txnIndex.file.getName)
  }

  /**
   * 更改此日志段的索引和日志文件的后缀
   * IOException from this method should be handled by the caller
   */
  def changeFileSuffixes(oldSuffix: String, newSuffix: String) {
    log.renameTo(new File(CoreUtils.replaceSuffix(log.file.getPath, oldSuffix, newSuffix)))
    offsetIndex.renameTo(new File(CoreUtils.replaceSuffix(offsetIndex.file.getPath, oldSuffix, newSuffix)))
    timeIndex.renameTo(new File(CoreUtils.replaceSuffix(timeIndex.file.getPath, oldSuffix, newSuffix)))
    txnIndex.renameTo(new File(CoreUtils.replaceSuffix(txnIndex.file.getPath, oldSuffix, newSuffix)))
  }

  /**
    * 变成闲置的分段
    *将最大时间索引条目附加到时间索引并修剪日志和索引。
   * 附加的时间索引条目将用于决定何时删除段。
   */
  def onBecomeInactiveSegment() {
    timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestamp, skipFullCheck = true)
    offsetIndex.trimToValidSize()
    timeIndex.trimToValidSize()
    log.trim()
  }

  /**
   * The time this segment has waited to be rolled.
   * If the first message batch has a timestamp we use its timestamp to determine when to roll a segment. A segment
   * is rolled if the difference between the new batch's timestamp and the first batch's timestamp exceeds the
   * segment rolling time.
   * If the first batch does not have a timestamp, we use the wall clock time to determine when to roll a segment. A
   * segment is rolled if the difference between the current wall clock time and the segment create time exceeds the
   * segment rolling time.
   */
  def timeWaitedForRoll(now: Long, messageTimestamp: Long) : Long = {
    // 将第一条消息的时间戳加载到内存中
    if (rollingBasedTimestamp.isEmpty) {
      val iter = log.batches.iterator()
      if (iter.hasNext)
        rollingBasedTimestamp = Some(iter.next().maxTimestamp)
    }
    rollingBasedTimestamp match {
      case Some(t) if t >= 0 => messageTimestamp - t
      case _ => now - created
    }
  }

  /**
   * Search the message offset based on timestamp and offset.
   *
   * This method returns an option of TimestampOffset. The returned value is determined using the following ordered list of rules:
   *
   * - If all the messages in the segment have smaller offsets, return None
   * - If all the messages in the segment have smaller timestamps, return None
   * - If all the messages in the segment have larger timestamps, or no message in the segment has a timestamp
   *   the returned the offset will be max(the base offset of the segment, startingOffset) and the timestamp will be Message.NoTimestamp.
   * - Otherwise, return an option of TimestampOffset. The offset is the offset of the first message whose timestamp
   *   is greater than or equals to the target timestamp and whose offset is greater than or equals to the startingOffset.
   *
   * This methods only returns None when 1) all messages' offset < startOffing or 2) the log is not empty but we did not
   * see any message when scanning the log from the indexed position. The latter could happen if the log is truncated
   * after we get the indexed position but before we scan the log from there. In this case we simply return None and the
   * caller will need to check on the truncated log and maybe retry or even do the search on another log segment.
   *
   * @param timestamp The timestamp to search for.
   * @param startingOffset The starting offset to search.
   * @return the timestamp and offset of the first message that meets the requirements. None will be returned if there is no such message.
   */
  def findOffsetByTimestamp(timestamp: Long, startingOffset: Long = baseOffset): Option[TimestampOffset] = {
    // Get the index entry with a timestamp less than or equal to the target timestamp
    val timestampOffset = timeIndex.lookup(timestamp)
    val position = offsetIndex.lookup(math.max(timestampOffset.offset, startingOffset)).position

    // Search the timestamp
    Option(log.searchForTimestamp(timestamp, position, startingOffset)).map { timestampAndOffset =>
      TimestampOffset(timestampAndOffset.timestamp, timestampAndOffset.offset)
    }
  }

  /**
   * Close this log segment
   */
  def close() {
    CoreUtils.swallow(timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestamp, skipFullCheck = true), this)
    CoreUtils.swallow(offsetIndex.close(), this)
    CoreUtils.swallow(timeIndex.close(), this)
    CoreUtils.swallow(log.close(), this)
    CoreUtils.swallow(txnIndex.close(), this)
  }

  /**
    * 关闭日志段使用的文件处理程序，但不写入磁盘。 这在磁盘可能失败时使用
    */
  def closeHandlers() {
    CoreUtils.swallow(offsetIndex.closeHandler(), this)
    CoreUtils.swallow(timeIndex.closeHandler(), this)
    CoreUtils.swallow(log.closeHandlers(), this)
    CoreUtils.swallow(txnIndex.close(), this)
  }

  /**
   * Delete this log segment from the filesystem.
   */
  def deleteIfExists() {
    def delete(delete: () => Boolean, fileType: String, file: File, logIfMissing: Boolean): Unit = {
      try {
        if (delete())
          info(s"Deleted $fileType ${file.getAbsolutePath}.")
        else if (logIfMissing)
          info(s"Failed to delete $fileType ${file.getAbsolutePath} because it does not exist.")
      }
      catch {
        case e: IOException => throw new IOException(s"Delete of $fileType ${file.getAbsolutePath} failed.", e)
      }
    }

    CoreUtils.tryAll(Seq(
      () => delete(log.deleteIfExists _, "log", log.file, logIfMissing = true),
      () => delete(offsetIndex.deleteIfExists _, "offset index", offsetIndex.file, logIfMissing = true),
      () => delete(timeIndex.deleteIfExists _, "time index", timeIndex.file, logIfMissing = true),
      () => delete(txnIndex.deleteIfExists _, "transaction index", txnIndex.file, logIfMissing = false)
    ))
  }

  /**
   * The last modified time of this log segment as a unix time stamp
   */
  def lastModified = log.file.lastModified

  /**
   * The largest timestamp this segment contains.
   */
  def largestTimestamp = if (maxTimestampSoFar >= 0) maxTimestampSoFar else lastModified

  /**
   * Change the last modified time for this log segment
   */
  def lastModified_=(ms: Long) = {
    val fileTime = FileTime.fromMillis(ms)
    Files.setLastModifiedTime(log.file.toPath, fileTime)
    Files.setLastModifiedTime(offsetIndex.file.toPath, fileTime)
    Files.setLastModifiedTime(timeIndex.file.toPath, fileTime)
  }
}

object LogSegment {

  def open(dir: File, baseOffset: Long, config: LogConfig, time: Time, fileAlreadyExists: Boolean = false,
           initFileSize: Int = 0, preallocate: Boolean = false, fileSuffix: String = ""): LogSegment = {
    val maxIndexSize = config.maxIndexSize
    new LogSegment(
      FileRecords.open(Log.logFile(dir, baseOffset, fileSuffix), fileAlreadyExists, initFileSize, preallocate),
      new OffsetIndex(Log.offsetIndexFile(dir, baseOffset, fileSuffix), baseOffset = baseOffset, maxIndexSize = maxIndexSize),
      new TimeIndex(Log.timeIndexFile(dir, baseOffset, fileSuffix), baseOffset = baseOffset, maxIndexSize = maxIndexSize),
      new TransactionIndex(baseOffset, Log.transactionIndexFile(dir, baseOffset, fileSuffix)),
      baseOffset,
      indexIntervalBytes = config.indexInterval,
      rollJitterMs = config.randomSegmentJitter,
      maxSegmentMs = config.segmentMs,
      maxSegmentBytes = config.segmentSize,
      time)
  }

}

object LogFlushStats extends KafkaMetricsGroup {
  val logFlushTimer = new KafkaTimer(newTimer("LogFlushRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
}
