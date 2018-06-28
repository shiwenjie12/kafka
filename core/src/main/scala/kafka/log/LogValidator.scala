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

import java.nio.ByteBuffer

import kafka.common.LongRef
import kafka.message.{CompressionCodec, NoCompressionCodec}
import kafka.utils.Logging
import org.apache.kafka.common.errors.{InvalidTimestampException, UnsupportedForMessageFormatException}
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.Time

import scala.collection.mutable
import scala.collection.JavaConverters._

private[kafka] object LogValidator extends Logging {

  /**
   * 更新此消息集的偏移量，并对消息进行进一步验证，包括：
   * 1.紧凑主题的消息必须有密钥
   * 2.当魔术值> = 1时，压缩消息集的内部消息必须从0开始单调增加偏移量。
   * 3.当魔术值> = 1时，验证并可能覆盖消息的时间戳。
   * 4. DefaultRecordBatch中声明的记录数必须与其中包含的有效记录数相匹配。
   *
   * 此方法将根据需要将消息转换为主题配置的消息格式版本。
    * 如果消息不需要格式转换或值覆盖，则此方法将执行就地操作以避免昂贵的重新压缩。
   *
   * Returns a ValidationAndOffsetAssignResult containing the validated message set, maximum timestamp, the offset
   * of the shallow message with the max timestamp and a boolean indicating whether the message sizes may have changed.
   */
  private[kafka] def validateMessagesAndAssignOffsets(records: MemoryRecords,
                                                      offsetCounter: LongRef,
                                                      time: Time,
                                                      now: Long,
                                                      sourceCodec: CompressionCodec,
                                                      targetCodec: CompressionCodec,
                                                      compactedTopic: Boolean, // 是否压缩主题
                                                      magic: Byte, // 服务器的消息magic
                                                      timestampType: TimestampType,
                                                      timestampDiffMaxMs: Long,
                                                      partitionLeaderEpoch: Int,
                                                      isFromClient: Boolean): ValidationAndOffsetAssignResult = {
    if (sourceCodec == NoCompressionCodec && targetCodec == NoCompressionCodec) { // 都是无压缩版本
      // check the magic value
      if (!records.hasMatchingMagic(magic)) // 验证magic，生产者不符合服务器，进行转换
        convertAndAssignOffsetsNonCompressed(records, offsetCounter, compactedTopic, time, now, timestampType,
          timestampDiffMaxMs, magic, partitionLeaderEpoch, isFromClient)
      else
        // Do in-place validation, offset assignment and maybe set timestamp
        assignOffsetsNonCompressed(records, offsetCounter, now, compactedTopic, timestampType, timestampDiffMaxMs,
          partitionLeaderEpoch, isFromClient, magic)
    } else {
      validateMessagesAndAssignOffsetsCompressed(records, offsetCounter, time, now, sourceCodec, targetCodec, compactedTopic,
        magic, timestampType, timestampDiffMaxMs, partitionLeaderEpoch, isFromClient)
    }
  }

  // 验证批次
  private def validateBatch(batch: RecordBatch, isFromClient: Boolean, toMagic: Byte): Unit = {
    if (isFromClient) {
      if (batch.magic >= RecordBatch.MAGIC_VALUE_V2) {
        val countFromOffsets = batch.lastOffset - batch.baseOffset + 1
        if (countFromOffsets <= 0)
          throw new InvalidRecordException(s"Batch has an invalid offset range: [${batch.baseOffset}, ${batch.lastOffset}]")

        // v2 and above messages always have a non-null count
        val count = batch.countOrNull
        if (count <= 0)
          throw new InvalidRecordException(s"Invalid reported count for record batch: $count")

        if (countFromOffsets != batch.countOrNull)
          throw new InvalidRecordException(s"Inconsistent batch offset range [${batch.baseOffset}, ${batch.lastOffset}] " +
            s"and count of records $count")
      }

      if (batch.hasProducerId && batch.baseSequence < 0)
        throw new InvalidRecordException(s"Invalid sequence number ${batch.baseSequence} in record batch " +
          s"with producerId ${batch.producerId}")

      if (batch.isControlBatch)
        throw new InvalidRecordException("Clients are not allowed to write control records")
    }

    if (batch.isTransactional && toMagic < RecordBatch.MAGIC_VALUE_V2)
      throw new UnsupportedForMessageFormatException(s"Transactional records cannot be used with magic version $toMagic")

    if (batch.hasProducerId && toMagic < RecordBatch.MAGIC_VALUE_V2)
      throw new UnsupportedForMessageFormatException(s"Idempotent records cannot be used with magic version $toMagic")
  }

  // 验证记录
  private def validateRecord(batch: RecordBatch, record: Record, now: Long, timestampType: TimestampType,
                             timestampDiffMaxMs: Long, compactedTopic: Boolean): Unit = {
    if (!record.hasMagic(batch.magic))
      throw new InvalidRecordException(s"Log record magic does not match outer magic ${batch.magic}")

    // verify the record-level CRC only if this is one of the deep entries of a compressed message
    // set for magic v0 and v1. For non-compressed messages, there is no inner record for magic v0 and v1,
    // so we depend on the batch-level CRC check in Log.analyzeAndValidateRecords(). For magic v2 and above,
    // there is no record-level CRC to check.
    if (batch.magic <= RecordBatch.MAGIC_VALUE_V1 && batch.isCompressed)
      record.ensureValid()

    validateKey(record, compactedTopic)
    validateTimestamp(batch, record, now, timestampType, timestampDiffMaxMs)
  }

  private def convertAndAssignOffsetsNonCompressed(records: MemoryRecords,
                                                   offsetCounter: LongRef,
                                                   compactedTopic: Boolean,
                                                   time: Time,
                                                   now: Long,
                                                   timestampType: TimestampType,
                                                   timestampDiffMaxMs: Long,
                                                   toMagicValue: Byte,
                                                   partitionLeaderEpoch: Int,
                                                   isFromClient: Boolean): ValidationAndOffsetAssignResult = {
    val sizeInBytesAfterConversion = AbstractRecords.estimateSizeInBytes(toMagicValue, offsetCounter.value,
      CompressionType.NONE, records.records) // 转换版本之后，根据压缩类型，估计大小

    val (producerId, producerEpoch, sequence, isTransactional) = {
      val first = records.batches.asScala.head
      (first.producerId, first.producerEpoch, first.baseSequence, first.isTransactional)
    }

    val newBuffer = ByteBuffer.allocate(sizeInBytesAfterConversion) // 分配估计内存
    // 构建构造器
    val builder = MemoryRecords.builder(newBuffer, toMagicValue, CompressionType.NONE, timestampType,
      offsetCounter.value, now, producerId, producerEpoch, sequence, isTransactional, partitionLeaderEpoch)

    for (batch <- records.batches.asScala) {
      validateBatch(batch, isFromClient, toMagicValue)

      for (record <- batch.asScala) {
        validateRecord(batch, record, now, timestampType, timestampDiffMaxMs, compactedTopic)
        builder.appendWithOffset(offsetCounter.getAndIncrement(), record) // 添加带有偏移量的记录
      }
    }

    val convertedRecords = builder.build() // 生成内存记录

    val info = builder.info
    val recordsProcessingStats = new RecordsProcessingStats(builder.uncompressedBytesWritten,
      builder.numRecords, time.nanoseconds - now)
    ValidationAndOffsetAssignResult(
      validatedRecords = convertedRecords,
      maxTimestamp = info.maxTimestamp,
      shallowOffsetOfMaxTimestamp = info.shallowOffsetOfMaxTimestamp,
      messageSizeMaybeChanged = true,
      recordsProcessingStats = recordsProcessingStats)
  }

  // 主要是用于分配偏移量属性
  private def assignOffsetsNonCompressed(records: MemoryRecords,
                                         offsetCounter: LongRef,
                                         now: Long,
                                         compactedTopic: Boolean,
                                         timestampType: TimestampType,
                                         timestampDiffMaxMs: Long,
                                         partitionLeaderEpoch: Int,
                                         isFromClient: Boolean,
                                         magic: Byte): ValidationAndOffsetAssignResult = {
    var maxTimestamp = RecordBatch.NO_TIMESTAMP
    var offsetOfMaxTimestamp = -1L
    val initialOffset = offsetCounter.value

    for (batch <- records.batches.asScala) { // 设置批次的相关属性
      validateBatch(batch, isFromClient, magic)

      var maxBatchTimestamp = RecordBatch.NO_TIMESTAMP
      var offsetOfMaxBatchTimestamp = -1L

      for (record <- batch.asScala) {
        validateRecord(batch, record, now, timestampType, timestampDiffMaxMs, compactedTopic)
        val offset = offsetCounter.getAndIncrement()
        if (batch.magic > RecordBatch.MAGIC_VALUE_V0 && record.timestamp > maxBatchTimestamp) {
          maxBatchTimestamp = record.timestamp
          offsetOfMaxBatchTimestamp = offset
        }
      }

      if (batch.magic > RecordBatch.MAGIC_VALUE_V0 && maxBatchTimestamp > maxTimestamp) {
        maxTimestamp = maxBatchTimestamp
        offsetOfMaxTimestamp = offsetOfMaxBatchTimestamp
      }

      batch.setLastOffset(offsetCounter.value - 1)

      if (batch.magic >= RecordBatch.MAGIC_VALUE_V2)
        batch.setPartitionLeaderEpoch(partitionLeaderEpoch)

      if (batch.magic > RecordBatch.MAGIC_VALUE_V0) {
        if (timestampType == TimestampType.LOG_APPEND_TIME)
          batch.setMaxTimestamp(TimestampType.LOG_APPEND_TIME, now)
        else
          batch.setMaxTimestamp(timestampType, maxBatchTimestamp)
      }
    }

    if (timestampType == TimestampType.LOG_APPEND_TIME) {
      maxTimestamp = now
      if (magic >= RecordBatch.MAGIC_VALUE_V2)
        offsetOfMaxTimestamp = offsetCounter.value - 1
      else
        offsetOfMaxTimestamp = initialOffset
    }

    ValidationAndOffsetAssignResult(
      validatedRecords = records,
      maxTimestamp = maxTimestamp,
      shallowOffsetOfMaxTimestamp = offsetOfMaxTimestamp,
      messageSizeMaybeChanged = false,
      recordsProcessingStats = RecordsProcessingStats.EMPTY)
  }

  /**
   * 在以下情况之一中，我们无法进行地点分配：
   * 1.源和目标压缩编解码器不同
   * 2.当使用的魔法值为0时，因为偏移量需要被覆盖
   * 3.当要使用的魔法值大于0时，内部消息的某些字段需要被覆盖。
   * 4.需要消息格式转换。
   */
  def validateMessagesAndAssignOffsetsCompressed(records: MemoryRecords,
                                                 offsetCounter: LongRef,
                                                 time: Time,
                                                 now: Long,
                                                 sourceCodec: CompressionCodec,
                                                 targetCodec: CompressionCodec,
                                                 compactedTopic: Boolean,
                                                 toMagic: Byte,
                                                 timestampType: TimestampType,
                                                 timestampDiffMaxMs: Long,
                                                 partitionLeaderEpoch: Int,
                                                 isFromClient: Boolean): ValidationAndOffsetAssignResult = {

      // No in place assignment situation 1 and 2
      var inPlaceAssignment = sourceCodec == targetCodec && toMagic > RecordBatch.MAGIC_VALUE_V0

      var maxTimestamp = RecordBatch.NO_TIMESTAMP
      val expectedInnerOffset = new LongRef(0) // 预期的偏移量
      val validatedRecords = new mutable.ArrayBuffer[Record]

      var uncompressedSizeInBytes = 0

      for (batch <- records.batches.asScala) {
        validateBatch(batch, isFromClient, toMagic)
        uncompressedSizeInBytes += AbstractRecords.recordBatchHeaderSizeInBytes(toMagic, batch.compressionType())

        // Do not compress control records unless they are written compressed
        if (sourceCodec == NoCompressionCodec && batch.isControlBatch)
          inPlaceAssignment = true

        for (record <- batch.asScala) {
          validateRecord(batch, record, now, timestampType, timestampDiffMaxMs, compactedTopic)
          if (sourceCodec != NoCompressionCodec && record.isCompressed)
            throw new InvalidRecordException("Compressed outer record should not have an inner record with a " +
              s"compression attribute set: $record")

          uncompressedSizeInBytes += record.sizeInBytes()
          if (batch.magic > RecordBatch.MAGIC_VALUE_V0 && toMagic > RecordBatch.MAGIC_VALUE_V0) {
            // Check if we need to overwrite offset
            // No in place assignment situation 3
            if (record.offset != expectedInnerOffset.getAndIncrement())
              inPlaceAssignment = false
            if (record.timestamp > maxTimestamp)
              maxTimestamp = record.timestamp
          }

          // No in place assignment situation 4
          if (!record.hasMagic(toMagic))
            inPlaceAssignment = false

          validatedRecords += record
        }
      }

      if (!inPlaceAssignment) { // 需要重新分配，解压，再压缩
        val (producerId, producerEpoch, sequence, isTransactional) = {
          // note that we only reassign offsets for requests coming straight from a producer. For records with magic V2,
          // there should be exactly one RecordBatch per request, so the following is all we need to do. For Records
          // with older magic versions, there will never be a producer id, etc.
          val first = records.batches.asScala.head
          (first.producerId, first.producerEpoch, first.baseSequence, first.isTransactional)
        }
        buildRecordsAndAssignOffsets(toMagic, offsetCounter, time, timestampType, CompressionType.forId(targetCodec.codec), now,
          validatedRecords, producerId, producerEpoch, sequence, isTransactional, partitionLeaderEpoch, isFromClient,
          uncompressedSizeInBytes)
      } else {
        // 我们只能更新批处理并按原样写入压缩的有效内容
        val batch = records.batches.iterator.next()
        val lastOffset = offsetCounter.addAndGet(validatedRecords.size) - 1

        batch.setLastOffset(lastOffset)

        if (timestampType == TimestampType.LOG_APPEND_TIME)
          maxTimestamp = now

        if (toMagic >= RecordBatch.MAGIC_VALUE_V1)
          batch.setMaxTimestamp(timestampType, maxTimestamp)

        if (toMagic >= RecordBatch.MAGIC_VALUE_V2)
          batch.setPartitionLeaderEpoch(partitionLeaderEpoch)

        val recordsProcessingStats = new RecordsProcessingStats(uncompressedSizeInBytes, 0, -1)
        ValidationAndOffsetAssignResult(validatedRecords = records,
          maxTimestamp = maxTimestamp,
          shallowOffsetOfMaxTimestamp = lastOffset,
          messageSizeMaybeChanged = false,
          recordsProcessingStats = recordsProcessingStats)
      }
  }

  private def buildRecordsAndAssignOffsets(magic: Byte,
                                           offsetCounter: LongRef,
                                           time: Time,
                                           timestampType: TimestampType,
                                           compressionType: CompressionType,
                                           logAppendTime: Long,
                                           validatedRecords: Seq[Record],
                                           producerId: Long,
                                           producerEpoch: Short,
                                           baseSequence: Int,
                                           isTransactional: Boolean,
                                           partitionLeaderEpoch: Int,
                                           isFromClient: Boolean,
                                           uncompresssedSizeInBytes: Int): ValidationAndOffsetAssignResult = {
    val startNanos = time.nanoseconds
    val estimatedSize = AbstractRecords.estimateSizeInBytes(magic, offsetCounter.value, compressionType,
      validatedRecords.asJava) // 估计大小
    val buffer = ByteBuffer.allocate(estimatedSize)
    val builder = MemoryRecords.builder(buffer, magic, compressionType, timestampType, offsetCounter.value,
      logAppendTime, producerId, producerEpoch, baseSequence, isTransactional, partitionLeaderEpoch)

    validatedRecords.foreach { record =>
      builder.appendWithOffset(offsetCounter.getAndIncrement(), record)
    }

    val records = builder.build()

    val info = builder.info

    // This is not strictly correct, it represents the number of records where in-place assignment is not possible
    // instead of the number of records that were converted. It will over-count cases where the source and target are
    // message format V0 or if the inner offsets are not consecutive. This is OK since the impact is the same: we have
    // to rebuild the records (including recompression if enabled).
    val conversionCount = builder.numRecords
    val recordsProcessingStats = new RecordsProcessingStats(uncompresssedSizeInBytes + builder.uncompressedBytesWritten,
      conversionCount, time.nanoseconds - startNanos)

    ValidationAndOffsetAssignResult(
      validatedRecords = records,
      maxTimestamp = info.maxTimestamp,
      shallowOffsetOfMaxTimestamp = info.shallowOffsetOfMaxTimestamp,
      messageSizeMaybeChanged = true,
      recordsProcessingStats = recordsProcessingStats)
  }

  // 验证是否压缩key
  private def validateKey(record: Record, compactedTopic: Boolean) {
    if (compactedTopic && !record.hasKey)
      throw new InvalidRecordException("Compacted topic cannot accept message without key.")
  }

  /**
   * This method validates the timestamps of a message.
   * If the message is using create time, this method checks if it is within acceptable range.
   */
  private def validateTimestamp(batch: RecordBatch,
                                record: Record,
                                now: Long,
                                timestampType: TimestampType,
                                timestampDiffMaxMs: Long) {
    if (timestampType == TimestampType.CREATE_TIME
      && record.timestamp != RecordBatch.NO_TIMESTAMP
      && math.abs(record.timestamp - now) > timestampDiffMaxMs) // 时间戳是否超过范围
      throw new InvalidTimestampException(s"Timestamp ${record.timestamp} of message with offset ${record.offset} is " +
        s"out of range. The timestamp should be within [${now - timestampDiffMaxMs}, ${now + timestampDiffMaxMs}]")
    if (batch.timestampType == TimestampType.LOG_APPEND_TIME) // 生产者的时间戳不应该是日志添加类型的
      throw new InvalidTimestampException(s"Invalid timestamp type in message $record. Producer should not set " +
        s"timestamp type to LogAppendTime.")
  }

  case class ValidationAndOffsetAssignResult(validatedRecords: MemoryRecords,
                                             maxTimestamp: Long,
                                             shallowOffsetOfMaxTimestamp: Long,
                                             messageSizeMaybeChanged: Boolean,
                                             recordsProcessingStats: RecordsProcessingStats)

}
