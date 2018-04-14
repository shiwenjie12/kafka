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
package org.apache.kafka.common.record;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static org.apache.kafka.common.utils.Utils.wrapNullable;

/**
 * This class is used to write new log data in memory, i.e. this is the write path for {@link MemoryRecords}.
 * It transparently handles compression and exposes methods for appending new records, possibly with message
 * format conversion.
 *
 * In cases where keeping memory retention low is important and there's a gap between the time that record appends stop
 * and the builder is closed (e.g. the Producer), it's important to call `closeForRecordAppends` when the former happens.
 * This will release resources like compression buffers that can be relatively large (64 KB for LZ4).
 */
public class MemoryRecordsBuilder {
    private static final float COMPRESSION_RATE_ESTIMATION_FACTOR = 1.05f;
    private static final DataOutputStream CLOSED_STREAM = new DataOutputStream(new OutputStream() {
        @Override
        public void write(int b) throws IOException {
            throw new IllegalStateException("MemoryRecordsBuilder is closed for record appends");
        }
    });

    private final TimestampType timestampType;
    private final CompressionType compressionType;
    // Used to hold a reference to the underlying ByteBuffer so that we can write the record batch header and access
    // the written bytes. ByteBufferOutputStream allocates a new ByteBuffer if the existing one is not large enough,
    // so it's not safe to hold a direct reference to the underlying ByteBuffer.
    private final ByteBufferOutputStream bufferStream;
    private final byte magic;
    private final int initialPosition;
    private final long baseOffset;
    private final long logAppendTime;
    private final boolean isControlBatch;
    private final int partitionLeaderEpoch;
    private final int writeLimit;
    private final int batchHeaderSizeInBytes;

    // Use a conservative estimate of the compression ratio. The producer overrides this using statistics
    // from previous batches before appending any records.
    private float estimatedCompressionRatio = 1.0F;

    // 用于附加记录，可以压缩数据。
    private DataOutputStream appendStream;
    private boolean isTransactional;
    private long producerId;
    private short producerEpoch;
    private int baseSequence;
    /**
     * 为压缩的记录大小
     */
    private int uncompressedRecordsSizeInBytes = 0; // Number of bytes (excluding the header) written before compression
    /**
     * 记录数
     */
    private int numRecords = 0;
    /**
     * 实际的压缩比例
     */
    private float actualCompressionRatio = 1;
    private long maxTimestamp = RecordBatch.NO_TIMESTAMP;
    private long offsetOfMaxTimestamp = -1;
    /**
     * 最新偏移量
     */
    private Long lastOffset = null;
    private Long firstTimestamp = null;

    private MemoryRecords builtRecords;
    private boolean aborted = false;

    public MemoryRecordsBuilder(ByteBufferOutputStream bufferStream,
                                byte magic,
                                CompressionType compressionType,
                                TimestampType timestampType,
                                long baseOffset,
                                long logAppendTime,
                                long producerId,
                                short producerEpoch,
                                int baseSequence,
                                boolean isTransactional,
                                boolean isControlBatch,
                                int partitionLeaderEpoch,
                                int writeLimit) {
        if (magic > RecordBatch.MAGIC_VALUE_V0 && timestampType == TimestampType.NO_TIMESTAMP_TYPE)
            throw new IllegalArgumentException("TimestampType must be set for magic >= 0");
        if (magic < RecordBatch.MAGIC_VALUE_V2) {
            if (isTransactional)
                throw new IllegalArgumentException("Transactional records are not supported for magic " + magic);
            if (isControlBatch)
                throw new IllegalArgumentException("Control records are not supported for magic " + magic);
        }

        this.magic = magic;
        this.timestampType = timestampType;
        this.compressionType = compressionType;
        this.baseOffset = baseOffset;
        this.logAppendTime = logAppendTime;
        this.numRecords = 0;
        this.uncompressedRecordsSizeInBytes = 0;
        this.actualCompressionRatio = 1;
        this.maxTimestamp = RecordBatch.NO_TIMESTAMP;
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.baseSequence = baseSequence;
        this.isTransactional = isTransactional;
        this.isControlBatch = isControlBatch;
        this.partitionLeaderEpoch = partitionLeaderEpoch;
        this.writeLimit = writeLimit;
        this.initialPosition = bufferStream.position();
        this.batchHeaderSizeInBytes = AbstractRecords.recordBatchHeaderSizeInBytes(magic, compressionType);

        bufferStream.position(initialPosition + batchHeaderSizeInBytes);
        this.bufferStream = bufferStream;
        this.appendStream = new DataOutputStream(compressionType.wrapForOutput(this.bufferStream, magic));
    }

    /**
     * Construct a new builder.
     *
     * @param buffer 要使用的底层缓冲区（注意，该类将在必要时分配一个新的缓冲区以适应附加的记录）
     * @param magic 使用的 magic 值
     * @param compressionType 要使用的压缩编解码器
     * @param timestampType 所需的时间戳类型. For magic > 0, this cannot be {@link TimestampType#NO_TIMESTAMP_TYPE}.
     * @param baseOffset 要使用的初始偏移量
     * @param logAppendTime 此记录集的日志追加时间。 Can be set to NO_TIMESTAMP if CREATE_TIME is used.
     * @param producerId 与生成记录集的生产商关联的生产者ID
     * @param producerEpoch The epoch of the producer
     * @param baseSequence 该集合中第一个记录的序列号
     * @param isTransactional 记录是否是事务的一部分
     * @param isControlBatch 这是否是控制批次 (e.g. for transaction markers)
     * @param partitionLeaderEpoch 分区领导者将记录集追加到日志的Epoch。
     * @param writeLimit The desired limit on the total bytes for this record set (note that this can be exceeded
     *                   when compression is used since size estimates are rough, and in the case that the first
     *                   record added exceeds the size).
     */
    public MemoryRecordsBuilder(ByteBuffer buffer,
                                byte magic,
                                CompressionType compressionType,
                                TimestampType timestampType,
                                long baseOffset,
                                long logAppendTime,
                                long producerId,
                                short producerEpoch,
                                int baseSequence,
                                boolean isTransactional,
                                boolean isControlBatch,
                                int partitionLeaderEpoch,
                                int writeLimit) {
        this(new ByteBufferOutputStream(buffer), magic, compressionType, timestampType, baseOffset, logAppendTime,
                producerId, producerEpoch, baseSequence, isTransactional, isControlBatch, partitionLeaderEpoch,
                writeLimit);
    }

    public ByteBuffer buffer() {
        return bufferStream.buffer();
    }

    public int initialCapacity() {
        return bufferStream.initialCapacity();
    }

    public double compressionRatio() {
        return actualCompressionRatio;
    }

    public CompressionType compressionType() {
        return compressionType;
    }

    public boolean isControlBatch() {
        return isControlBatch;
    }

    public boolean isTransactional() {
        return isTransactional;
    }

    /**
     * 关闭此生成器并返回结果缓冲区。
     * @return The built log buffer
     */
    public MemoryRecords build() {
        if (aborted) {
            throw new IllegalStateException("Attempting to build an aborted record batch");
        }
        close();
        return builtRecords;
    }

    /**
     * Get the max timestamp and its offset. The details of the offset returned are a bit subtle.
     *
     * If the log append time is used, the offset will be the last offset unless no compression is used and
     * the message format version is 0 or 1, in which case, it will be the first offset.
     *
     * If create time is used, the offset will be the last offset unless no compression is used and the message
     * format version is 0 or 1, in which case, it will be the offset of the record with the max timestamp.
     *
     * @return The max timestamp and its offset
     */
    public RecordsInfo info() {
        if (timestampType == TimestampType.LOG_APPEND_TIME) {
            long shallowOffsetOfMaxTimestamp;
            // Use the last offset when dealing with record batches
            if (compressionType != CompressionType.NONE || magic >= RecordBatch.MAGIC_VALUE_V2)
                shallowOffsetOfMaxTimestamp = lastOffset;
            else
                shallowOffsetOfMaxTimestamp = baseOffset;
            return new RecordsInfo(logAppendTime, shallowOffsetOfMaxTimestamp);
        } else if (maxTimestamp == RecordBatch.NO_TIMESTAMP) {
            return new RecordsInfo(RecordBatch.NO_TIMESTAMP, lastOffset);
        } else {
            long shallowOffsetOfMaxTimestamp;
            // Use the last offset when dealing with record batches
            if (compressionType != CompressionType.NONE || magic >= RecordBatch.MAGIC_VALUE_V2)
                shallowOffsetOfMaxTimestamp = lastOffset;
            else
                shallowOffsetOfMaxTimestamp = offsetOfMaxTimestamp;
            return new RecordsInfo(maxTimestamp, shallowOffsetOfMaxTimestamp);
        }
    }

    public int numRecords() {
        return numRecords;
    }

    /**
     * Return the sum of the size of the batch header (always uncompressed) and the records (before compression).
     */
    public int uncompressedBytesWritten() {
        return uncompressedRecordsSizeInBytes + batchHeaderSizeInBytes;
    }

    public void setProducerState(long producerId, short producerEpoch, int baseSequence, boolean isTransactional) {
        if (isClosed()) {
            // Sequence numbers are assigned when the batch is closed while the accumulator is being drained.
            // If the resulting ProduceRequest to the partition leader failed for a retriable error, the batch will
            // be re queued. In this case, we should not attempt to set the state again, since changing the producerId and sequence
            // once a batch has been sent to the broker risks introducing duplicates.
            throw new IllegalStateException("Trying to set producer state of an already closed batch. This indicates a bug on the client.");
        }
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.baseSequence = baseSequence;
        this.isTransactional = isTransactional;
    }

    public void overrideLastOffset(long lastOffset) {
        if (builtRecords != null)
            throw new IllegalStateException("Cannot override the last offset after the records have been built");
        this.lastOffset = lastOffset;
    }

    /**
     * 释放所需的资源用于记录追加（如压缩缓冲区）。一旦调用此方法，以更新RecordBatch头唯一的可能。
     */
    public void closeForRecordAppends() {
        if (appendStream != CLOSED_STREAM) {
            try {
                appendStream.close();
            } catch (IOException e) {
                throw new KafkaException(e);
            } finally {
                appendStream = CLOSED_STREAM;
            }
        }
    }

    public void abort() {
        closeForRecordAppends();
        buffer().position(initialPosition);
        aborted = true;
    }

    public void reopenAndRewriteProducerState(long producerId, short producerEpoch, int baseSequence, boolean isTransactional) {
        if (aborted)
            throw new IllegalStateException("Should not reopen a batch which is already aborted.");
        builtRecords = null;
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.baseSequence = baseSequence;
        this.isTransactional = isTransactional;
    }


    public void close() {
        if (aborted)
            throw new IllegalStateException("Cannot close MemoryRecordsBuilder as it has already been aborted");

        if (builtRecords != null)
            return;

        validateProducerState();

        closeForRecordAppends();

        if (numRecords == 0L) {
            buffer().position(initialPosition);
            builtRecords = MemoryRecords.EMPTY;
        } else {
            if (magic > RecordBatch.MAGIC_VALUE_V1)
                this.actualCompressionRatio = (float) writeDefaultBatchHeader() / this.uncompressedRecordsSizeInBytes;
            else if (compressionType != CompressionType.NONE)
                this.actualCompressionRatio = (float) writeLegacyCompressedWrapperHeader() / this.uncompressedRecordsSizeInBytes;

            ByteBuffer buffer = buffer().duplicate();
            buffer.flip();
            buffer.position(initialPosition);
            builtRecords = MemoryRecords.readableRecords(buffer.slice());
        }
    }

    /**
     * 验证生产者状态
     */
    private void validateProducerState() {
        if (isTransactional && producerId == RecordBatch.NO_PRODUCER_ID)
            throw new IllegalArgumentException("Cannot write transactional messages without a valid producer ID");

        if (producerId != RecordBatch.NO_PRODUCER_ID) {
            if (producerEpoch == RecordBatch.NO_PRODUCER_EPOCH)
                throw new IllegalArgumentException("Invalid negative producer epoch");

            if (baseSequence < 0 && !isControlBatch)
                throw new IllegalArgumentException("Invalid negative sequence number used");

            if (magic < RecordBatch.MAGIC_VALUE_V2)
                throw new IllegalArgumentException("Idempotent messages are not supported for magic " + magic);
        }
    }

    /**
     * 写入默认批（V2）的头
     * @return the written compressed bytes.
     */
    private int writeDefaultBatchHeader() {
        ensureOpenForRecordBatchWrite();
        ByteBuffer buffer = bufferStream.buffer();
        int pos = buffer.position();
        buffer.position(initialPosition);
        int size = pos - initialPosition;
        int writtenCompressed = size - DefaultRecordBatch.RECORD_BATCH_OVERHEAD;
        int offsetDelta = (int) (lastOffset - baseOffset);

        final long maxTimestamp;
        if (timestampType == TimestampType.LOG_APPEND_TIME)
            maxTimestamp = logAppendTime;
        else
            maxTimestamp = this.maxTimestamp;

        DefaultRecordBatch.writeHeader(buffer, baseOffset, offsetDelta, size, magic, compressionType, timestampType,
                firstTimestamp, maxTimestamp, producerId, producerEpoch, baseSequence, isTransactional, isControlBatch,
                partitionLeaderEpoch, numRecords);

        buffer.position(pos);
        return writtenCompressed;
    }

    /**
     * Write the header to the legacy batch.
     * @return the written compressed bytes.
     */
    private int writeLegacyCompressedWrapperHeader() {
        ensureOpenForRecordBatchWrite();
        ByteBuffer buffer = bufferStream.buffer();
        int pos = buffer.position();
        buffer.position(initialPosition);

        int wrapperSize = pos - initialPosition - Records.LOG_OVERHEAD;
        int writtenCompressed = wrapperSize - LegacyRecord.recordOverhead(magic);
        AbstractLegacyRecordBatch.writeHeader(buffer, lastOffset, wrapperSize);

        long timestamp = timestampType == TimestampType.LOG_APPEND_TIME ? logAppendTime : maxTimestamp;
        LegacyRecord.writeCompressedRecordHeader(buffer, magic, wrapperSize, timestamp, compressionType, timestampType);

        buffer.position(pos);
        return writtenCompressed;
    }

    /**
     * 追加记录和返回的消息格式V0和V1的校验，或null的V2及以上。
     */
    private Long appendWithOffset(long offset, boolean isControlRecord, long timestamp, ByteBuffer key,
                                  ByteBuffer value, Header[] headers) {
        try {
            if (isControlRecord != isControlBatch)
                throw new IllegalArgumentException("Control records can only be appended to control batches");

            if (lastOffset != null && offset <= lastOffset)
                throw new IllegalArgumentException(String.format("Illegal offset %s following previous offset %s " +
                        "(Offsets must increase monotonically).", offset, lastOffset));

            if (timestamp < 0 && timestamp != RecordBatch.NO_TIMESTAMP)
                throw new IllegalArgumentException("Invalid negative timestamp " + timestamp);

            if (magic < RecordBatch.MAGIC_VALUE_V2 && headers != null && headers.length > 0)
                throw new IllegalArgumentException("Magic v" + magic + " does not support record headers");

            if (firstTimestamp == null)
                firstTimestamp = timestamp;

            if (magic > RecordBatch.MAGIC_VALUE_V1) {// V2
                appendDefaultRecord(offset, timestamp, key, value, headers);
                return null;
            } else {// V1、V0
                return appendLegacyRecord(offset, timestamp, key, value);
            }
        } catch (IOException e) {
            throw new KafkaException("I/O exception when writing to the append stream, closing", e);
        }
    }

    /**
     * Append a new record at the given offset.
     * @param offset The absolute offset of the record in the log buffer
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @param headers The record headers if there are any
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long appendWithOffset(long offset, long timestamp, byte[] key, byte[] value, Header[] headers) {
        return appendWithOffset(offset, false, timestamp, wrapNullable(key), wrapNullable(value), headers);
    }

    /**
     * Append a new record at the given offset.
     * @param offset The absolute offset of the record in the log buffer
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @param headers The record headers if there are any
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long appendWithOffset(long offset, long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers) {
        return appendWithOffset(offset, false, timestamp, key, value, headers);
    }

    /**
     * Append a new record at the given offset.
     * @param offset The absolute offset of the record in the log buffer
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long appendWithOffset(long offset, long timestamp, byte[] key, byte[] value) {
        return appendWithOffset(offset, timestamp, wrapNullable(key), wrapNullable(value), Record.EMPTY_HEADERS);
    }

    /**
     * Append a new record at the given offset.
     * @param offset The absolute offset of the record in the log buffer
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long appendWithOffset(long offset, long timestamp, ByteBuffer key, ByteBuffer value) {
        return appendWithOffset(offset, timestamp, key, value, Record.EMPTY_HEADERS);
    }

    /**
     * Append a new record at the given offset.
     * @param offset The absolute offset of the record in the log buffer
     * @param record The record to append
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long appendWithOffset(long offset, SimpleRecord record) {
        return appendWithOffset(offset, record.timestamp(), record.key(), record.value(), record.headers());
    }

    /**
     * Append a new record at the next sequential offset.
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long append(long timestamp, ByteBuffer key, ByteBuffer value) {
        return append(timestamp, key, value, Record.EMPTY_HEADERS);
    }

    /**
     * Append a new record at the next sequential offset.
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @param headers The record headers if there are any
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long append(long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers) {
        return appendWithOffset(nextSequentialOffset(), timestamp, key, value, headers);
    }

    /**
     * 在下一个序列偏移量上追加一条新记录。
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long append(long timestamp, byte[] key, byte[] value) {
        return append(timestamp, wrapNullable(key), wrapNullable(value), Record.EMPTY_HEADERS);
    }

    /**
     * Append a new record at the next sequential offset.
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @param headers The record headers if there are any
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long append(long timestamp, byte[] key, byte[] value, Header[] headers) {
        return append(timestamp, wrapNullable(key), wrapNullable(value), headers);
    }

    /**
     * Append a new record at the next sequential offset.
     * @param record The record to append
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long append(SimpleRecord record) {
        return appendWithOffset(nextSequentialOffset(), record);
    }

    /**
     * Append a control record at the next sequential offset.
     * @param timestamp The record timestamp
     * @param type The control record type (cannot be UNKNOWN)
     * @param value The control record value
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    private Long appendControlRecord(long timestamp, ControlRecordType type, ByteBuffer value) {
        Struct keyStruct = type.recordKey();
        ByteBuffer key = ByteBuffer.allocate(keyStruct.sizeOf());
        keyStruct.writeTo(key);
        key.flip();
        return appendWithOffset(nextSequentialOffset(), true, timestamp, key, value, Record.EMPTY_HEADERS);
    }

    /**
     * Return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long appendEndTxnMarker(long timestamp, EndTransactionMarker marker) {
        if (producerId == RecordBatch.NO_PRODUCER_ID)
            throw new IllegalArgumentException("End transaction marker requires a valid producerId");
        if (!isTransactional)
            throw new IllegalArgumentException("End transaction marker depends on batch transactional flag being enabled");
        ByteBuffer value = marker.serializeValue();
        return appendControlRecord(timestamp, marker.controlType(), value);
    }

    /**
     * Add a legacy record without doing offset/magic validation (this should only be used in testing).
     * @param offset The offset of the record
     * @param record The record to add
     */
    public void appendUncheckedWithOffset(long offset, LegacyRecord record) {
        ensureOpenForRecordAppend();
        try {
            int size = record.sizeInBytes();
            AbstractLegacyRecordBatch.writeHeader(appendStream, toInnerOffset(offset), size);

            ByteBuffer buffer = record.buffer().duplicate();
            appendStream.write(buffer.array(), buffer.arrayOffset(), buffer.limit());

            recordWritten(offset, record.timestamp(), size + Records.LOG_OVERHEAD);
        } catch (IOException e) {
            throw new KafkaException("I/O exception when writing to the append stream, closing", e);
        }
    }

    /**
     * Append a record at the next sequential offset.
     * @param record the record to add
     */
    public void append(Record record) {
        appendWithOffset(record.offset(), isControlBatch, record.timestamp(), record.key(), record.value(), record.headers());
    }

    /**
     * Append a log record using a different offset
     * @param offset The offset of the record
     * @param record The record to add
     */
    public void appendWithOffset(long offset, Record record) {
        appendWithOffset(offset, record.timestamp(), record.key(), record.value(), record.headers());
    }

    /**
     * Add a record with a given offset. The record must have a magic which matches the magic use to
     * construct this builder and the offset must be greater than the last appended record.
     * @param offset The offset of the record
     * @param record The record to add
     */
    public void appendWithOffset(long offset, LegacyRecord record) {
        appendWithOffset(offset, record.timestamp(), record.key(), record.value());
    }

    /**
     * Append the record at the next consecutive offset. If no records have been appended yet, use the base
     * offset of this builder.
     * @param record The record to add
     */
    public void append(LegacyRecord record) {
        appendWithOffset(nextSequentialOffset(), record);
    }

    /**
     * V2 版本的添加记录
     * @param offset
     * @param timestamp
     * @param key
     * @param value
     * @param headers
     * @throws IOException
     */
    private void appendDefaultRecord(long offset, long timestamp, ByteBuffer key, ByteBuffer value,
                                     Header[] headers) throws IOException {
        ensureOpenForRecordAppend();
        int offsetDelta = (int) (offset - baseOffset);
        long timestampDelta = timestamp - firstTimestamp;
        int sizeInBytes = DefaultRecord.writeTo(appendStream, offsetDelta, timestampDelta, key, value, headers);
        recordWritten(offset, timestamp, sizeInBytes);
    }

    /**
     * 添加历史遗留的记录（V0、V1） 无header的版本
     * @param offset
     * @param timestamp
     * @param key
     * @param value
     * @return 返回此记录的crc32 统一校验码
     * @throws IOException
     */
    private long appendLegacyRecord(long offset, long timestamp, ByteBuffer key, ByteBuffer value) throws IOException {
        ensureOpenForRecordAppend();//  确保append stream 没有关闭
        if (compressionType == CompressionType.NONE && timestampType == TimestampType.LOG_APPEND_TIME)
            timestamp = logAppendTime;//?

        int size = LegacyRecord.recordSize(magic, key, value);// 记录长度
        AbstractLegacyRecordBatch.writeHeader(appendStream, toInnerOffset(offset), size);

        if (timestampType == TimestampType.LOG_APPEND_TIME)
            timestamp = logAppendTime;//?
        long crc = LegacyRecord.write(appendStream, magic, timestamp, key, value, CompressionType.NONE, timestampType);// 写入数据
        recordWritten(offset, timestamp, size + Records.LOG_OVERHEAD);
        return crc;
    }

    private long toInnerOffset(long offset) {
        // 用压缩V1使用压缩消息的相对偏移量
        if (magic > 0 && compressionType != CompressionType.NONE)
            return offset - baseOffset;
        return offset;
    }

    /**
     * 记录写入信息
     * @param offset
     * @param timestamp
     * @param size
     */
    private void recordWritten(long offset, long timestamp, int size) {
        if (numRecords == Integer.MAX_VALUE)
            throw new IllegalArgumentException("Maximum number of records per batch exceeded, max records: " + Integer.MAX_VALUE);
        if (offset - baseOffset > Integer.MAX_VALUE)
            throw new IllegalArgumentException("Maximum offset delta exceeded, base offset: " + baseOffset +
                    ", last offset: " + offset);

        numRecords += 1;
        uncompressedRecordsSizeInBytes += size;
        lastOffset = offset;

        if (magic > RecordBatch.MAGIC_VALUE_V0 && timestamp > maxTimestamp) {
            maxTimestamp = timestamp;
            offsetOfMaxTimestamp = offset;
        }
    }

    /**
     * 确保记录可以添加
     */
    private void ensureOpenForRecordAppend() {
        if (appendStream == CLOSED_STREAM)
            throw new IllegalStateException("Tried to append a record, but MemoryRecordsBuilder is closed for record appends");
    }

    private void ensureOpenForRecordBatchWrite() {
        if (isClosed())
            throw new IllegalStateException("Tried to write record batch header, but MemoryRecordsBuilder is closed");
        if (aborted)
            throw new IllegalStateException("Tried to write record batch header, but MemoryRecordsBuilder is aborted");
    }

    /**
     * Get an estimate of the number of bytes written (based on the estimation factor hard-coded in {@link CompressionType}.
     * @return The estimated number of bytes written
     */
    private int estimatedBytesWritten() {
        if (compressionType == CompressionType.NONE) {
            return batchHeaderSizeInBytes + uncompressedRecordsSizeInBytes;
        } else {
            // 根据未压缩的字节估计到底层字节缓冲区的写入字节。
            return batchHeaderSizeInBytes + (int) (uncompressedRecordsSizeInBytes * estimatedCompressionRatio * COMPRESSION_RATE_ESTIMATION_FACTOR);
        }
    }

    /**
     * 为内存记录生成器设置估计的压缩比。
     */
    public void setEstimatedCompressionRatio(float estimatedCompressionRatio) {
        this.estimatedCompressionRatio = estimatedCompressionRatio;
    }

    /**
     * 检查我们是否有一个包含给定键/值对的新记录的空间。如果没有记录附加，则返回true。
     */
    public boolean hasRoomFor(long timestamp, byte[] key, byte[] value, Header[] headers) {
        return hasRoomFor(timestamp, wrapNullable(key), wrapNullable(value), headers);
    }

    /**
     * Check if we have room for a new record containing the given key/value pair. If no records have been
     * appended, then this returns true.
     *
     * Note that the return value is based on the estimate of the bytes written to the compressor, which may not be
     * accurate if compression is used. When this happens, the following append may cause dynamic buffer
     * re-allocation in the underlying byte buffer stream.
     */
    public boolean hasRoomFor(long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers) {
        if (isFull())
            return false;

        // We always allow at least one record to be appended (the ByteBufferOutputStream will grow as needed)
        if (numRecords == 0)
            return true;

        final int recordSize;
        if (magic < RecordBatch.MAGIC_VALUE_V2) {
            recordSize = Records.LOG_OVERHEAD + LegacyRecord.recordSize(magic, key, value);
        } else {
            int nextOffsetDelta = lastOffset == null ? 0 : (int) (lastOffset - baseOffset + 1);
            long timestampDelta = firstTimestamp == null ? 0 : timestamp - firstTimestamp;
            recordSize = DefaultRecord.sizeInBytes(nextOffsetDelta, timestampDelta, key, value, headers);
        }

        // Be conservative and not take compression of the new record into consideration.
        return this.writeLimit >= estimatedBytesWritten() + recordSize;
    }

    public boolean isClosed() {
        return builtRecords != null;
    }

    public boolean isFull() {
        // note that the write limit is respected only after the first record is added which ensures we can always
        // create non-empty batches (this is used to disable batching when the producer's batch size is set to 0).
        return appendStream == CLOSED_STREAM || (this.numRecords > 0 && this.writeLimit <= estimatedBytesWritten());
    }

    /**
     * Get an estimate of the number of bytes written to the underlying buffer. The returned value
     * is exactly correct if the record set is not compressed or if the builder has been closed.
     */
    public int estimatedSizeInBytes() {
        return builtRecords != null ? builtRecords.sizeInBytes() : estimatedBytesWritten();
    }

    public byte magic() {
        return magic;
    }

    private long nextSequentialOffset() {
        return lastOffset == null ? baseOffset : lastOffset + 1;
    }

    public static class RecordsInfo {
        public final long maxTimestamp;
        public final long shallowOffsetOfMaxTimestamp;

        public RecordsInfo(long maxTimestamp,
                           long shallowOffsetOfMaxTimestamp) {
            this.maxTimestamp = maxTimestamp;
            this.shallowOffsetOfMaxTimestamp = shallowOffsetOfMaxTimestamp;
        }
    }

    /**
     * Return the producer id of the RecordBatches created by this builder.
     */
    public long producerId() {
        return this.producerId;
    }

    public short producerEpoch() {
        return this.producerEpoch;
    }

    public int baseSequence() {
        return this.baseSequence;
    }
}
