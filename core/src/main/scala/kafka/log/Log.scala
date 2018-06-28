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
import java.text.NumberFormat
import java.util.concurrent.atomic._
import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap, TimeUnit}

import kafka.api.KAFKA_0_10_0_IV0
import kafka.common.{InvalidOffsetException, KafkaException, LongRef}
import kafka.metrics.KafkaMetricsGroup
import kafka.server.{BrokerTopicStats, FetchDataInfo, LogDirFailureChannel, LogOffsetMetadata}
import kafka.utils._
import org.apache.kafka.common.errors.{CorruptRecordException, KafkaStorageException, OffsetOutOfRangeException, RecordBatchTooLargeException, RecordTooLargeException, UnsupportedForMessageFormatException}
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.{IsolationLevel, ListOffsetRequest}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.collection.{Seq, Set, mutable}
import com.yammer.metrics.core.Gauge
import org.apache.kafka.common.utils.{Time, Utils}
import kafka.message.{BrokerCompressionCodec, CompressionCodec, NoCompressionCodec}
import kafka.server.checkpoints.{LeaderEpochCheckpointFile, LeaderEpochFile}
import kafka.server.epoch.{LeaderEpochCache, LeaderEpochFileCache}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.FetchResponse.AbortedTransaction
import java.util.Map.{Entry => JEntry}
import java.lang.{Long => JLong}
import java.util.regex.Pattern

object LogAppendInfo {
  val UnknownLogAppendInfo = LogAppendInfo(-1, -1, RecordBatch.NO_TIMESTAMP, -1L, RecordBatch.NO_TIMESTAMP, -1L,
    RecordsProcessingStats.EMPTY, NoCompressionCodec, NoCompressionCodec, -1, -1, offsetsMonotonic = false)

  def unknownLogAppendInfoWithLogStartOffset(logStartOffset: Long): LogAppendInfo =
    LogAppendInfo(-1, -1, RecordBatch.NO_TIMESTAMP, -1L, RecordBatch.NO_TIMESTAMP, logStartOffset,
      RecordsProcessingStats.EMPTY, NoCompressionCodec, NoCompressionCodec, -1, -1, offsetsMonotonic = false)
}

/**
 * 在追加到日志之前，Struct用于保存关于每个消息集的各种数量
 *
 * @param firstOffset 消息集中的第一个偏移量，除非消息格式小于V2并且我们追加到跟随者。
  *                    在这种情况下，这将是性能原因的最后一个抵消。
 * @param lastOffset 消息集中的最后一个偏移量
 * @param maxTimestamp 消息集的最大时间戳。
 * @param offsetOfMaxTimestamp 消息与最大时间戳的偏移量。
 * @param logAppendTime 日志追加消息集的时间（如果使用），否则为Message.NoTimestamp
 * @param logStartOffset 此追加时间日志的起始偏移量。
 * @param recordsProcessingStats 记录处理期间收集的统计信息，如果assignOffsets为false，则为null
 * @param sourceCodec 消息集中使用的源编解码器（由生产者发送）
 * @param targetCodec 消息集的目标编解码器（在应用代理压缩配置之后（如果有的话）
 * @param shallowCount The number of shallow messages
 * @param validBytes 有效字节的数量
 * @param offsetsMonotonic 这个消息集中的偏移量是单调递增的
 */
case class LogAppendInfo(var firstOffset: Long,
                         var lastOffset: Long,
                         var maxTimestamp: Long,
                         var offsetOfMaxTimestamp: Long,
                         var logAppendTime: Long,
                         var logStartOffset: Long,
                         var recordsProcessingStats: RecordsProcessingStats,
                         sourceCodec: CompressionCodec,
                         targetCodec: CompressionCodec,
                         shallowCount: Int,
                         validBytes: Int,
                         offsetsMonotonic: Boolean)

/**
  * 用于保存有关已完成事务的有用元数据的类。这是用来在附加到日志之后构建事务索引的。
 *
 * @param producerId The ID of the producer
 * @param firstOffset 事务的第一个偏移量（包含）
 * @param lastOffset 事务的最后偏移（包括）。 这通常是指示事务完成的COMMIT / ABORT控制记录的偏移量。
 * @param isAborted 事务是否是中断的
 */
case class CompletedTxn(producerId: Long, firstOffset: Long, lastOffset: Long, isAborted: Boolean) {
  override def toString: String = {
    "CompletedTxn(" +
      s"producerId=$producerId, " +
      s"firstOffset=$firstOffset, " +
      s"lastOffset=$lastOffset, " +
      s"isAborted=$isAborted)"
  }
}

/**
 * 只存储用于存储消息的日志。
 *
 * 日志是一个日志段序列，每个日志段都有一个基偏移，表示该段中的第一条消息。
 * 根据一个可配置的策略创建新的日志段，该策略控制给定段的字节或时间间隔的大小。
 *
 * @param dir The directory in which log segments are created.
 * @param config The log configuration settings
 * @param logStartOffset The earliest offset allowed to be exposed to kafka client.
 *                       The logStartOffset can be updated by :
 *                       - user's DeleteRecordsRequest
 *                       - broker's log retention
 *                       - broker's log truncation
 *                       The logStartOffset is used to decide the following:
 *                       - Log deletion. LogSegment whose nextOffset <= log's logStartOffset can be deleted.
 *                         It may trigger log rolling if the active segment is deleted.
 *                       - Earliest offset of the log in response to ListOffsetRequest. To avoid OffsetOutOfRange exception after user seeks to earliest offset,
 *                         we make sure that logStartOffset <= log's highWatermark
 *                       Other activities such as log cleaning are not affected by logStartOffset.
 * @param recoveryPoint The offset at which to begin recovery--i.e. the first offset which has not been flushed to disk
 * @param scheduler The thread pool scheduler used for background actions
 * @param brokerTopicStats Container for Broker Topic Yammer Metrics
 * @param time The time instance used for checking the clock
 * @param maxProducerIdExpirationMs The maximum amount of time to wait before a producer id is considered expired
 * @param producerIdExpirationCheckIntervalMs How often to check for producer ids which need to be expired
 */
@threadsafe
class Log(@volatile var dir: File,
          @volatile var config: LogConfig,
          @volatile var logStartOffset: Long,// TopicPartition
          @volatile var recoveryPoint: Long,
          scheduler: Scheduler,
          brokerTopicStats: BrokerTopicStats,
          time: Time,
          val maxProducerIdExpirationMs: Int,
          val producerIdExpirationCheckIntervalMs: Int,
          val topicPartition: TopicPartition,
          val producerStateManager: ProducerStateManager,
          logDirFailureChannel: LogDirFailureChannel) extends Logging with KafkaMetricsGroup {

  import kafka.log.Log._

  /* A lock that guards all modifications to the log */
  private val lock = new Object
  // The memory mapped buffer for index files of this log will be closed for index files of this log will be closed with either delete() or closeHandlers()
  // After memory mapped buffer is closed, no disk IO operation should be performed for this log
  @volatile private var isMemoryMappedBufferClosed = false

  /* 上次刷新的时间 */
  private val lastFlushedTime = new AtomicLong(time.milliseconds)

  def initFileSize: Int = {// 初始化的文件大小
    if (config.preallocate)// 是否是预分配额
      config.segmentSize
    else
      0
  }

  def updateConfig(updatedKeys: Set[String], newConfig: LogConfig): Unit = {
    if ((updatedKeys.contains(LogConfig.RetentionMsProp)
      || updatedKeys.contains(LogConfig.MessageTimestampDifferenceMaxMsProp))
      && topicPartition.partition == 0  // generate warnings only for one partition of each topic
      && newConfig.retentionMs < newConfig.messageTimestampDifferenceMaxMs)
      warn(s"${LogConfig.RetentionMsProp} for topic ${topicPartition.topic} is set to ${newConfig.retentionMs}. It is smaller than " +
        s"${LogConfig.MessageTimestampDifferenceMaxMsProp}'s value ${newConfig.messageTimestampDifferenceMaxMs}. " +
        s"This may result in frequent log rolling.")
    this.config = newConfig
  }

  private def checkIfMemoryMappedBufferClosed(): Unit = {
    if (isMemoryMappedBufferClosed)
      throw new KafkaStorageException(s"The memory mapped buffer for log of $topicPartition is already closed")
  }

  @volatile private var nextOffsetMetadata: LogOffsetMetadata = _

  /* The earliest offset which is part of an incomplete transaction. This is used to compute the
   * last stable offset (LSO) in ReplicaManager. Note that it is possible that the "true" first unstable offset
   * gets removed from the log (through record or segment deletion). In this case, the first unstable offset
   * will point to the log start offset, which may actually be either part of a completed transaction or not
   * part of a transaction at all. However, since we only use the LSO for the purpose of restricting the
   * read_committed consumer to fetching decided data (i.e. committed, aborted, or non-transactional), this
   * temporary abuse seems justifiable and saves us from scanning the log after deletion to find the first offsets
   * of each ongoing transaction in order to compute a new first unstable offset. It is possible, however,
   * that this could result in disagreement between replicas depending on when they began replicating the log.
   * In the worst case, the LSO could be seen by a consumer to go backwards.
   * 作为不完整交易一部分的最早偏移量。
   * 这用于计算ReplicaManager中的最后一个稳定偏移量（LSO）。
   * 请注意，“真正的”第一个不稳定偏移量可能会从日志中删除（通过记录或段删除）。
   * 在这种情况下，第一个不稳定偏移量将指向日志起始偏移量，它实际上可能是完成的事务的一部分，或者根本不是事务的一部分。
    * 但是，由于我们仅使用LSO来限制read_committed使用者提取已确定的数据（即已提交，已中止或未事务处理），
    * 因此暂时的滥用似乎是合理的，并且可以避免在删除后扫描日志以找到第一个 为了计算新的第一个不稳定偏移量，
    * 每个正在进行的事务的偏移。 但是，这可能会导致副本之间出现分歧，具体取决于副本何时开始复制日志。
    * 在最糟糕的情况下，消费者可能会看到LSO倒退。
   */
  @volatile var firstUnstableOffset: Option[LogOffsetMetadata] = None

  /* Keep track of the current high watermark in order to ensure that segments containing offsets at or above it are
   * not eligible for deletion. This means that the active segment is only eligible for deletion if the high watermark
   * equals the log end offset (which may never happen for a partition under consistent load). This is needed to
   * prevent the log start offset (which is exposed in fetch responses) from getting ahead of the high watermark.
   * 跟踪当前的高水印，以确保包含在其上方或上方的偏移的片段不适合删除。
   * 这意味着，如果高水印等于日志结束偏移（对于一致负载下的分区可能永远不会发生），活动段只适合删除。
   * 这需要防止日志起始偏移（在获取响应中暴露），以避免在高水印之前。
   */
  @volatile private var replicaHighWatermark: Option[Long] = None

  /* the actual segments of the log */
  private val segments: ConcurrentNavigableMap[java.lang.Long, LogSegment] = new ConcurrentSkipListMap[java.lang.Long, LogSegment]

  @volatile private var _leaderEpochCache: LeaderEpochCache = initializeLeaderEpochCache()

  locally {
    val startMs = time.milliseconds

    val nextOffset = loadSegments()

    /* 计算下一条消息的偏移量 */
    nextOffsetMetadata = new LogOffsetMetadata(nextOffset, activeSegment.baseOffset, activeSegment.size)

    _leaderEpochCache.clearAndFlushLatest(nextOffsetMetadata.messageOffset)// 之后的

    logStartOffset = math.max(logStartOffset, segments.firstEntry.getValue.baseOffset)

    // The earliest leader epoch may not be flushed during a hard failure. Recover it here.
    _leaderEpochCache.clearAndFlushEarliest(logStartOffset)// 之前的

    loadProducerState(logEndOffset, reloadFromCleanShutdown = hasCleanShutdownFile)

    info("Completed load of log %s with %d log segments, log start offset %d and log end offset %d in %d ms"
      .format(name, segments.size(), logStartOffset, logEndOffset, time.milliseconds - startMs))
  }

  private val tags = {// 日志的标签
    val maybeFutureTag = if (isFuture) Map("is-future" -> "true") else Map.empty[String, String]
    Map("topic" -> topicPartition.topic, "partition" -> topicPartition.partition.toString) ++ maybeFutureTag
  }

  newGauge("NumLogSegments",
    new Gauge[Int] {
      def value = numberOfSegments
    },
    tags)

  newGauge("LogStartOffset",
    new Gauge[Long] {
      def value = logStartOffset
    },
    tags)

  newGauge("LogEndOffset",
    new Gauge[Long] {
      def value = logEndOffset
    },
    tags)

  newGauge("Size",
    new Gauge[Long] {
      def value = size
    },
    tags)

  scheduler.schedule(name = "PeriodicProducerExpirationCheck", fun = () => {
    lock synchronized {
      producerStateManager.removeExpiredProducers(time.milliseconds)
    }
  }, period = producerIdExpirationCheckIntervalMs, delay = producerIdExpirationCheckIntervalMs, unit = TimeUnit.MILLISECONDS)

  /** The name of this log */
  def name  = dir.getName()

  def leaderEpochCache = _leaderEpochCache

  // 初始化领导者epoch的缓存
  private def initializeLeaderEpochCache(): LeaderEpochCache = {
    // create the log directory if it doesn't exist
    Files.createDirectories(dir.toPath)
    new LeaderEpochFileCache(topicPartition, () => logEndOffsetMetadata,
      new LeaderEpochCheckpointFile(LeaderEpochFile.newFile(dir), logDirFailureChannel))
  }

  private def removeTempFilesAndCollectSwapFiles(): Set[File] = {

    // 删除批量索引文件
    def deleteIndicesIfExist(baseFile: File, swapFile: File, fileType: String): Unit = {
      info(s"Found $fileType file ${swapFile.getAbsolutePath} from interrupted swap operation. Deleting index files (if they exist).")
      val offset = offsetFromFile(baseFile)
      Files.deleteIfExists(Log.offsetIndexFile(dir, offset).toPath)
      Files.deleteIfExists(Log.timeIndexFile(dir, offset).toPath)
      Files.deleteIfExists(Log.transactionIndexFile(dir, offset).toPath)
    }

    var swapFiles = Set[File]()

    for (file <- dir.listFiles if file.isFile) {// 片段文件
      if (!file.canRead)
        throw new IOException(s"Could not read file $file")
      val filename = file.getName
      if (filename.endsWith(DeletedFileSuffix) || filename.endsWith(CleanedFileSuffix)) { // 删除杂散临时文件
        debug(s"Deleting stray temporary file ${file.getAbsolutePath}")
        Files.deleteIfExists(file.toPath)
      } else if (filename.endsWith(SwapFileSuffix)) {
        // 我们在交换操作中崩溃了，以恢复：
        //如果是日志，删除索引文件，稍后完成交换操作
        //如果索引只删除索引文件，它们将被重建。
        val baseFile = new File(CoreUtils.replaceSuffix(file.getPath, SwapFileSuffix, ""))// 获取原文件名称
        if (isIndexFile(baseFile)) {
          deleteIndicesIfExist(baseFile, file, "index")
        } else if (isLogFile(baseFile)) {
          deleteIndicesIfExist(baseFile, file, "log")
          swapFiles += file
        }
      }
    }
    swapFiles
  }

  //  此方法不需要将IOException转换为KafkaStorageException，因为它只在所有日志加载之前调用
  private def loadSegmentFiles(): Unit = {
    // 升序加载段，因为来自一个段的事务数据可能取决于它之前的段。
    for (file <- dir.listFiles.sortBy(_.getName) if file.isFile) {
      if (isIndexFile(file)) {
        // if it is an index file, make sure it has a corresponding .log file
        val offset = offsetFromFile(file)
        val logFile = Log.logFile(dir, offset)
        if (!logFile.exists) {// 如果日志文件不存在，则删除索引文件
          warn(s"Found an orphaned index file ${file.getAbsolutePath}, with no corresponding log file.")
          Files.deleteIfExists(file.toPath)
        }
      } else if (isLogFile(file)) {
        // if it's a log file, load the corresponding log segment
        val baseOffset = offsetFromFile(file) // 基本的偏移量
        val timeIndexFileNewlyCreated = !Log.timeIndexFile(dir, baseOffset).exists()// 新创建时间索引文件
        val segment = LogSegment.open(dir = dir,
          baseOffset = baseOffset,
          config,
          time = time,
          fileAlreadyExists = true)

        try segment.sanityCheck(timeIndexFileNewlyCreated)
        catch {// 如果出现问题，则恢复当前segment
          case _: NoSuchFileException =>
            error(s"Could not find offset index file corresponding to log file ${segment.log.file.getAbsolutePath}, " +
              "recovering segment and rebuilding index files...")
            recoverSegment(segment)
          case e: CorruptIndexException =>
            warn(s"Found a corrupted index file corresponding to log file ${segment.log.file.getAbsolutePath} due " +
              s"to ${e.getMessage}}, recovering segment and rebuilding index files...")
            recoverSegment(segment)
        }
        addSegment(segment)
      }
    }
  }

  // 恢复
  private def recoverSegment(segment: LogSegment, leaderEpochCache: Option[LeaderEpochCache] = None): Int = lock synchronized {
    val stateManager = new ProducerStateManager(topicPartition, dir, maxProducerIdExpirationMs)
    stateManager.truncateAndReload(logStartOffset, segment.baseOffset, time.milliseconds)// 截断segment之前的快照数据并加载
    logSegments(stateManager.mapEndOffset, segment.baseOffset).foreach { segment =>  //指定（tp的开始offset，当前segment的起始offset）范围的segment
      val startOffset = math.max(segment.baseOffset, stateManager.mapEndOffset)
      val fetchDataInfo = segment.read(startOffset, None, Int.MaxValue)// 读取现在的segment的数据实体
      if (fetchDataInfo != null)
        loadProducersFromLog(stateManager, fetchDataInfo.records)// 从日志中加载生产者状态
    }
    stateManager.updateMapEndOffset(segment.baseOffset)//  更新lastMapOffset

    // 为第一个恢复的段创建快照，以避免在检查点恢复点之前关闭所有段
    stateManager.takeSnapshot()// 打印快照

    val bytesTruncated = segment.recover(stateManager, leaderEpochCache)// 正式恢复当前的segment

    // 一旦我们恢复了该段的数据，拍摄快照以确保我们不需要在恢复另一段时重新加载相同的段。
    stateManager.takeSnapshot()
    bytesTruncated
  }

  // This method does not need to convert IOException to KafkaStorageException because it is only called before all logs are loaded
  // 完成对swap文件的操作
  private def completeSwapOperations(swapFiles: Set[File]): Unit = {
    for (swapFile <- swapFiles) {
      val logFile = new File(CoreUtils.replaceSuffix(swapFile.getPath, SwapFileSuffix, ""))
      val baseOffset = offsetFromFile(logFile)
      // 有swap文件构成的segment
      val swapSegment = LogSegment.open(swapFile.getParentFile,
        baseOffset = baseOffset,
        config,
        time = time,
        fileSuffix = SwapFileSuffix)
      info(s"Found log file ${swapFile.getPath} from interrupted swap operation, repairing.")
      recoverSegment(swapSegment)
      val oldSegments = logSegments(swapSegment.baseOffset, swapSegment.readNextOffset)
      replaceSegments(swapSegment, oldSegments.toSeq, isRecoveredSwapFile = true)
    }
  }

  // 从磁盘上的日志文件加载日志段并返回下一个偏移量。
  // 该方法不需要将IOException转换为KafkaStorageException异常，因为它只在加载所有日志之前调用。
  private def loadSegments(): Long = {
    // first do a pass through the files in the log directory and remove any temporary files
    // and find any interrupted swap operations
    val swapFiles = removeTempFilesAndCollectSwapFiles()

    // 现在执行第二遍并加载所有日志和索引文件
    loadSegmentFiles()

    // Finally, complete any interrupted swap operations. To be crash-safe,
    // log files that are replaced by the swap segment should be renamed to .deleted
    // before the swap file is restored as the new segment file.
    completeSwapOperations(swapFiles)

    if (logSegments.isEmpty) {
      // no existing segments, create a new mutable segment beginning at offset 0
      addSegment(LogSegment.open(dir = dir,
        baseOffset = 0,
        config,
        time = time,
        fileAlreadyExists = false,
        initFileSize = this.initFileSize,
        preallocate = config.preallocate))
      0
    } else if (!dir.getAbsolutePath.endsWith(Log.DeleteDirSuffix)) {
      val nextOffset = recoverLog()
      // 重置当前活动日志段的索引大小以允许更多条目
      activeSegment.resizeIndexes(config.maxIndexSize)
      nextOffset
    } else 0
  }

  // 更新接下来的偏移量元素
  private def updateLogEndOffset(messageOffset: Long) {
    nextOffsetMetadata = new LogOffsetMetadata(messageOffset, activeSegment.baseOffset, activeSegment.size)
  }

  /**
   * Recover the log segments and return the next offset after recovery.
   *
   * This method does not need to convert IOException to KafkaStorageException because it is only called before all
   * logs are loaded.
   */
  private def recoverLog(): Long = {
    // if we have the clean shutdown marker, skip recovery
    if (!hasCleanShutdownFile) {
      // okay we need to actually recover this log
      val unflushed = logSegments(this.recoveryPoint, Long.MaxValue).iterator
      while (unflushed.hasNext) {
        val segment = unflushed.next
        info("Recovering unflushed segment %d in log %s.".format(segment.baseOffset, name))
        val truncatedBytes =
          try {
            recoverSegment(segment, Some(_leaderEpochCache))
          } catch {
            case _: InvalidOffsetException =>
              val startOffset = segment.baseOffset
              warn("Found invalid offset during recovery for log " + dir.getName + ". Deleting the corrupt segment and " +
                "creating an empty one with starting offset " + startOffset)
              segment.truncateTo(startOffset)
          }
        if (truncatedBytes > 0) {
          // we had an invalid message, delete all remaining log
          warn("Corruption found in segment %d of log %s, truncating to offset %d.".format(segment.baseOffset, name,
            segment.readNextOffset))
          unflushed.foreach(deleteSegment)
        }
      }
    }
    recoveryPoint = activeSegment.readNextOffset
    recoveryPoint
  }

  // 加载有关生产者的状态
  private def loadProducerState(lastOffset: Long, reloadFromCleanShutdown: Boolean): Unit = lock synchronized {
    checkIfMemoryMappedBufferClosed()
    val messageFormatVersion = config.messageFormatVersion.messageFormatVersion.value
    info(s"Loading producer state from offset $lastOffset for partition $topicPartition with message " +
      s"format version $messageFormatVersion")

    // We want to avoid unnecessary scanning of the log to build the producer state when the broker is being
    // upgraded. The basic idea is to use the absence of producer snapshot files to detect the upgrade case,
    // but we have to be careful not to assume too much in the presence of broker failures. The two most common
    // upgrade cases in which we expect to find no snapshots are the following:
    // 我们希望在代理升级时避免不必要的日志扫描以建立生产者状态。
    // 基本思想是使用缺少生产者快照文件来检测升级情况，但是我们必须小心，不要在代理失败的情况下承担太多。
    // 我们希望找不到任何快照的两种最常见的升级情况如下：
    // 1. 经纪人已升级，但主题仍旧采用旧的消息格式。
    // 2. 代理已经升级，主题是新的消息格式，我们有一个干净的关闭。
    //
    // If we hit either of these cases, we skip producer state loading and write a new snapshot at the log end
    // offset (see below). The next time the log is reloaded, we will load producer state using this snapshot
    // (or later snapshots). Otherwise, if there is no snapshot file, then we have to rebuild producer state
    // from the first segment.

    if (producerStateManager.latestSnapshotOffset.isEmpty && (messageFormatVersion < RecordBatch.MAGIC_VALUE_V2 || reloadFromCleanShutdown)) {
      // To avoid an expensive scan through all of the segments, we take empty snapshots from the start of the
      // last two segments and the last offset. This should avoid the full scan in the case that the log needs
      // truncation.
      val nextLatestSegmentBaseOffset = lowerSegment(activeSegment.baseOffset).map(_.baseOffset)
      val offsetsToSnapshot = Seq(nextLatestSegmentBaseOffset, Some(activeSegment.baseOffset), Some(lastOffset))
      offsetsToSnapshot.flatten.foreach { offset =>
        producerStateManager.updateMapEndOffset(offset)
        producerStateManager.takeSnapshot()
      }
    } else {
      val isEmptyBeforeTruncation = producerStateManager.isEmpty && producerStateManager.mapEndOffset >= lastOffset
      producerStateManager.truncateAndReload(logStartOffset, lastOffset, time.milliseconds())

      // Only do the potentially expensive reloading if the last snapshot offset is lower than the log end
      // offset (which would be the case on first startup) and there were active producers prior to truncation
      // (which could be the case if truncating after initial loading). If there weren't, then truncating
      // shouldn't change that fact (although it could cause a producerId to expire earlier than expected),
      // and we can skip the loading. This is an optimization for users which are not yet using
      // idempotent/transactional features yet.
      if (lastOffset > producerStateManager.mapEndOffset && !isEmptyBeforeTruncation) {
        logSegments(producerStateManager.mapEndOffset, lastOffset).foreach { segment =>
          val startOffset = Utils.max(segment.baseOffset, producerStateManager.mapEndOffset, logStartOffset)
          producerStateManager.updateMapEndOffset(startOffset)
          producerStateManager.takeSnapshot()

          val fetchDataInfo = segment.read(startOffset, Some(lastOffset), Int.MaxValue)
          if (fetchDataInfo != null)
            loadProducersFromLog(producerStateManager, fetchDataInfo.records)
        }
      }

      producerStateManager.updateMapEndOffset(lastOffset)
      updateFirstUnstableOffset()
    }
  }

  // 从事务中加载生产者信息
  private def loadProducersFromLog(producerStateManager: ProducerStateManager, records: Records): Unit = {
    val loadedProducers = mutable.Map.empty[Long, ProducerAppendInfo]
    val completedTxns = ListBuffer.empty[CompletedTxn]
    records.batches.asScala.foreach { batch =>
      if (batch.hasProducerId) {
        val maybeCompletedTxn = updateProducers(batch, loadedProducers, isFromClient = false)
        maybeCompletedTxn.foreach(completedTxns += _)// 添加到完成事务中
      }
    }
    loadedProducers.values.foreach(producerStateManager.update)// 更新生产者的信息
    completedTxns.foreach(producerStateManager.completeTxn)// 对已完成的事务标记，进行处理
  }

  private[log] def activeProducersWithLastSequence: Map[Long, Int] = lock synchronized {
    producerStateManager.activeProducers.map { case (producerId, producerIdEntry) =>
      (producerId, producerIdEntry.lastSeq)
    }
  }

  /**
   * Check if we have the "clean shutdown" file
   */
  private def hasCleanShutdownFile: Boolean = new File(dir.getParentFile, CleanShutdownFile).exists()

  /**
   * The number of segments in the log.
   * Take care! this is an O(n) operation.
   */
  def numberOfSegments: Int = segments.size

  /**
   * Close this log.
   * The memory mapped buffer for index files of this log will be left open until the log is deleted.
   */
  def close() {
    debug(s"Closing log $name")
    lock synchronized {
      checkIfMemoryMappedBufferClosed()
      maybeHandleIOException(s"Error while renaming dir for $topicPartition in dir ${dir.getParent}") {
        // We take a snapshot at the last written offset to hopefully avoid the need to scan the log
        // after restarting and to ensure that we cannot inadvertently hit the upgrade optimization
        // (the clean shutdown file is written after the logs are all closed).
        producerStateManager.takeSnapshot()
        logSegments.foreach(_.close())
      }
    }
  }

  /**
    * Rename the directory of the log
    *
    * @throws KafkaStorageException if rename fails
    */
  def renameDir(name: String) {
    lock synchronized {
      maybeHandleIOException(s"Error while renaming dir for $topicPartition in log dir ${dir.getParent}") {
        val renamedDir = new File(dir.getParent, name)
        Utils.atomicMoveWithFallback(dir.toPath, renamedDir.toPath)
        if (renamedDir != dir) {
          dir = renamedDir
          logSegments.foreach(_.updateDir(renamedDir))
          producerStateManager.logDir = dir
          // re-initialize leader epoch cache so that LeaderEpochCheckpointFile.checkpoint can correctly reference
          // the checkpoint file in renamed log directory
          _leaderEpochCache = initializeLeaderEpochCache()
        }
      }
    }
  }

  /**
   * Close file handlers used by log but don't write to disk. This is called if the log directory is offline
   */
  def closeHandlers() {
    debug(s"Closing handlers of log $name")
    lock synchronized {
      logSegments.foreach(_.closeHandlers())
      isMemoryMappedBufferClosed = true
    }
  }

  /**
   * 将此消息集添加到日志的活动部分，分配偏移量和分区引导程序时间
   *
   * @param records The records to append
   * @param isFromClient Whether or not this append is from a producer
   * @throws KafkaStorageException If the append fails due to an I/O error.
   * @return Information about the appended messages including the first and last offset.
   */
  def appendAsLeader(records: MemoryRecords, leaderEpoch: Int, isFromClient: Boolean = true): LogAppendInfo = {
    append(records, isFromClient, assignOffsets = true, leaderEpoch)
  }

  /**
   * 将此消息集附加到日志的活动部分，而不分配偏移量或分区引导程序时期
   *
   * @param records The records to append
   * @throws KafkaStorageException If the append fails due to an I/O error.
   * @return Information about the appended messages including the first and last offset.
   */
  def appendAsFollower(records: MemoryRecords): LogAppendInfo = {
    append(records, isFromClient = false, assignOffsets = false, leaderEpoch = -1)
  }

  /**
    * 将此消息集附加到日志的活动部分，如有必要，请将其转移到新的部分。
    * 此方法通常负责为消息分配偏移量，但如果传递assignOffsets = false标志，我们将只检查现有偏移量是否有效。
   *
   * @param records The log records to append
   * @param isFromClient Whether or not this append is from a producer
   * @param assignOffsets 日志应该为这个消息集指定偏移量还是盲目应用它给出的内容
   * @param leaderEpoch The partition's leader epoch which will be applied to messages when offsets are assigned on the leader
   * @throws KafkaStorageException If the append fails due to an I/O error.
   * @return Information about the appended messages including the first and last offset.
   */
  private def append(records: MemoryRecords, isFromClient: Boolean, assignOffsets: Boolean, leaderEpoch: Int): LogAppendInfo = {
    maybeHandleIOException(s"Error while appending records to $topicPartition in dir ${dir.getParent}") {
      val appendInfo = analyzeAndValidateRecords(records, isFromClient = isFromClient)

      // 如果我们没有有效的消息或者如果这是最后附加条目的副本，则返回
      if (appendInfo.shallowCount == 0)
        return appendInfo

      // 在将任何无效字节或部分消息附加到磁盘日志之前修剪它们
      var validRecords = trimInvalidBytes(records, appendInfo)

      // they are valid, insert them in the log
      lock synchronized {
        checkIfMemoryMappedBufferClosed()
        if (assignOffsets) { // leader，分配偏移量
          // assign offsets to the message set
          val offset = new LongRef(nextOffsetMetadata.messageOffset)
          appendInfo.firstOffset = offset.value
          val now = time.milliseconds
          val validateAndOffsetAssignResult = try { // 偏移量分配结果
            LogValidator.validateMessagesAndAssignOffsets(validRecords,
              offset,
              time,
              now,
              appendInfo.sourceCodec,
              appendInfo.targetCodec,
              config.compact,
              config.messageFormatVersion.messageFormatVersion.value,
              config.messageTimestampType,
              config.messageTimestampDifferenceMaxMs,
              leaderEpoch,
              isFromClient)
          } catch {
            case e: IOException => throw new KafkaException("Error in validating messages while appending to log '%s'".format(name), e)
          }
          validRecords = validateAndOffsetAssignResult.validatedRecords
          appendInfo.maxTimestamp = validateAndOffsetAssignResult.maxTimestamp
          appendInfo.offsetOfMaxTimestamp = validateAndOffsetAssignResult.shallowOffsetOfMaxTimestamp
          appendInfo.lastOffset = offset.value - 1
          appendInfo.recordsProcessingStats = validateAndOffsetAssignResult.recordsProcessingStats
          if (config.messageTimestampType == TimestampType.LOG_APPEND_TIME)
            appendInfo.logAppendTime = now

          // re-validate message sizes if there's a possibility that they have changed (due to re-compression or message
          // format conversion)
          if (validateAndOffsetAssignResult.messageSizeMaybeChanged) { // 表示可能重新分配
            for (batch <- validRecords.batches.asScala) {
              if (batch.sizeInBytes > config.maxMessageSize) { // 超过最大批大小
                // we record the original message set size instead of the trimmed size
                // to be consistent with pre-compression bytesRejectedRate recording
                brokerTopicStats.topicStats(topicPartition.topic).bytesRejectedRate.mark(records.sizeInBytes)
                brokerTopicStats.allTopicsStats.bytesRejectedRate.mark(records.sizeInBytes)
                throw new RecordTooLargeException("Message batch size is %d bytes which exceeds the maximum configured size of %d."
                  .format(batch.sizeInBytes, config.maxMessageSize))
              }
            }
          }
        } else { // follower,不需要分配偏移量
          // we are taking the offsets we are given
          if (!appendInfo.offsetsMonotonic || appendInfo.firstOffset < nextOffsetMetadata.messageOffset)
            throw new IllegalArgumentException("Out of order offsets found in " + records.records.asScala.map(_.offset))
        }

        // 通过领导者将消息戳记到消息中更新历元缓存
        validRecords.batches.asScala.foreach { batch =>
          if (batch.magic >= RecordBatch.MAGIC_VALUE_V2)
            _leaderEpochCache.assign(batch.partitionLeaderEpoch, batch.baseOffset)
        }

        // 检查消息集大小可能超过配置。
        if (validRecords.sizeInBytes > config.segmentSize) {
          throw new RecordBatchTooLargeException("Message batch size is %d bytes which exceeds the maximum configured segment size of %d."
            .format(validRecords.sizeInBytes, config.segmentSize))
        }

        // 现在我们有了有效的记录，分配了偏移量和更新了时间戳，我们需要验证生产者的幂等/事务状态并收集一些元数据
        val (updatedProducers, completedTxns, maybeDuplicate) = analyzeAndValidateProducerState(validRecords, isFromClient)
        maybeDuplicate.foreach { duplicate => // 从客户端的批次元数据
          appendInfo.firstOffset = duplicate.firstOffset
          appendInfo.lastOffset = duplicate.lastOffset
          appendInfo.logAppendTime = duplicate.timestamp
          appendInfo.logStartOffset = logStartOffset
          return appendInfo
        }

        // maybe roll the log if this segment is full
        val segment = maybeRoll(messagesSize = validRecords.sizeInBytes,
          maxTimestampInMessages = appendInfo.maxTimestamp,
          maxOffsetInMessages = appendInfo.lastOffset)

        val logOffsetMetadata = LogOffsetMetadata(
          messageOffset = appendInfo.firstOffset,
          segmentBaseOffset = segment.baseOffset,
          relativePositionInSegment = segment.size)

        // 向分段添加记录
        segment.append(firstOffset = appendInfo.firstOffset,
          largestOffset = appendInfo.lastOffset,
          largestTimestamp = appendInfo.maxTimestamp,
          shallowOffsetOfMaxTimestamp = appendInfo.offsetOfMaxTimestamp,
          records = validRecords)

        // update the producer state
        for ((producerId, producerAppendInfo) <- updatedProducers) {
          producerAppendInfo.maybeCacheTxnFirstOffsetMetadata(logOffsetMetadata) // 缓存生产者最新的事务偏移量元素
          producerStateManager.update(producerAppendInfo)
        }

        // 使用真正的最后一个稳定偏移更新事务索引。 消费者使用READ_COMMITTED可见的最后一个偏移量将受此值和高水位限制。
        for (completedTxn <- completedTxns) {
          val lastStableOffset = producerStateManager.completeTxn(completedTxn)
          segment.updateTxnIndex(completedTxn, lastStableOffset) // 更新事务索引
        }

        // always update the last producer id map offset so that the snapshot reflects the current offset
        // even if there isn't any idempotent data being written
        producerStateManager.updateMapEndOffset(appendInfo.lastOffset + 1)

        // 增加日志结束偏移量
        updateLogEndOffset(appendInfo.lastOffset + 1)

        // 更新第一个不稳定偏移量（用于计算LSO）
        updateFirstUnstableOffset()

        trace("Appended message set to log %s with first offset: %d, next offset: %d, and messages: %s"
          .format(this.name, appendInfo.firstOffset, nextOffsetMetadata.messageOffset, validRecords))

        if (unflushedMessages >= config.flushInterval)
          flush()

        appendInfo
      }
    }
  }

  def onHighWatermarkIncremented(highWatermark: Long): Unit = {
    lock synchronized {
      replicaHighWatermark = Some(highWatermark)
      producerStateManager.onHighWatermarkUpdated(highWatermark) // 设置高水印更新，删除水印之前已完成
      updateFirstUnstableOffset()
    }
  }

  //更新不稳定的偏移量
  private def updateFirstUnstableOffset(): Unit = lock synchronized {
    checkIfMemoryMappedBufferClosed()
    val updatedFirstStableOffset = producerStateManager.firstUnstableOffset match {
        // 只有满足offset是初始化（未知的）的或者小于当前log的起始offset，才会更新
      case Some(logOffsetMetadata) if logOffsetMetadata.messageOffsetOnly || logOffsetMetadata.messageOffset < logStartOffset =>
        val offset = math.max(logOffsetMetadata.messageOffset, logStartOffset)// 当前偏移量
        val segment = segments.floorEntry(offset).getValue //  当前segment
        val position  = segment.translateOffset(offset)// segment中的偏移量
        Some(LogOffsetMetadata(offset, segment.baseOffset, position.position))
      case other => other
    }

    if (updatedFirstStableOffset != this.firstUnstableOffset) { // 更新不稳定的偏移量
      debug(s"First unstable offset for ${this.name} updated to $updatedFirstStableOffset")
      this.firstUnstableOffset = updatedFirstStableOffset
    }
  }

  /**
   * 如果提供的偏移量较大，则增加日志开始偏移量。
   */
  def maybeIncrementLogStartOffset(newLogStartOffset: Long) {
    // We don't have to write the log start offset to log-start-offset-checkpoint immediately.
    // The deleteRecordsOffset may be lost only if all in-sync replicas of this broker are shutdown
    // in an unclean manner within log.flush.start.offset.checkpoint.interval.ms. The chance of this happening is low.
    maybeHandleIOException(s"Exception while increasing log start offset for $topicPartition to $newLogStartOffset in dir ${dir.getParent}") {
      lock synchronized {
        checkIfMemoryMappedBufferClosed()
        if (newLogStartOffset > logStartOffset) {
          info(s"Incrementing log start offset of partition $topicPartition to $newLogStartOffset in dir ${dir.getParent}")
          logStartOffset = newLogStartOffset
          _leaderEpochCache.clearAndFlushEarliest(logStartOffset)
          producerStateManager.truncateHead(logStartOffset)
          updateFirstUnstableOffset()
        }
      }
    }
  }

  private def analyzeAndValidateProducerState(records: MemoryRecords, isFromClient: Boolean):
  (mutable.Map[Long, ProducerAppendInfo], List[CompletedTxn], Option[BatchMetadata]) = {
    val updatedProducers = mutable.Map.empty[Long, ProducerAppendInfo]
    val completedTxns = ListBuffer.empty[CompletedTxn]
    for (batch <- records.batches.asScala if batch.hasProducerId) {
      val maybeLastEntry = producerStateManager.lastEntry(batch.producerId)

      // 如果这是一个客户端生产请求，将会有多达5批可以复制。？
      // 如果找到一个副本，我们将附加批处理的元数据返回给客户端。
      if (isFromClient)
        maybeLastEntry.flatMap(_.duplicateOf(batch)).foreach { duplicate =>
          return (updatedProducers, completedTxns.toList, Some(duplicate))
        }

      // 返回完成的事务，并添加到事务列表中
      val maybeCompletedTxn = updateProducers(batch, updatedProducers, isFromClient = isFromClient)
      maybeCompletedTxn.foreach(completedTxns += _)
    }
    (updatedProducers, completedTxns.toList, None)
  }

  /**
   * 验证以下内容：
   * <ol>
   * <li> 每条消息都与其CRC相匹配
   * <li> 每封邮件大小都是有效的
   * <li> 即进入记录批次的序列号与现有状态相互一致。
   * </ol>
   *
   * 还要计算以下数量：
   * <ol>
   * <li> 首先在消息集中偏移
   * <li> 消息集中的最后偏移量
   * <li> 消息的数量
   * <li> 有效字节数
   * <li> 偏移是否单调递增
   * <li> 是否使用任何压缩编解码器（如果使用了许多压缩编解码器，则给出最后一个）
   * </ol>
   */
  private def analyzeAndValidateRecords(records: MemoryRecords, isFromClient: Boolean): LogAppendInfo = {
    var shallowMessageCount = 0
    var validBytesCount = 0
    var firstOffset = -1L
    var lastOffset = -1L
    var sourceCodec: CompressionCodec = NoCompressionCodec
    var monotonic = true
    var maxTimestamp = RecordBatch.NO_TIMESTAMP
    var offsetOfMaxTimestamp = -1L

    for (batch <- records.batches.asScala) {
      // 我们只验证V2和更高版本以避免与旧客户端的潜在兼容性问题
      if (batch.magic >= RecordBatch.MAGIC_VALUE_V2 && isFromClient && batch.baseOffset != 0)
        throw new InvalidRecordException(s"The baseOffset of the record batch should be 0, but it is ${batch.baseOffset}")

      // update the first offset if on the first message. For magic versions older than 2, we use the last offset
      // to avoid the need to decompress the data (the last offset can be obtained directly from the wrapper message).
      // For magic version 2, we can get the first offset directly from the batch header.
      // When appending to the leader, we will update LogAppendInfo.baseOffset with the correct value. In the follower
      // case, validation will be more lenient.
      // 计算起始偏移量
      if (firstOffset < 0)
        firstOffset = if (batch.magic >= RecordBatch.MAGIC_VALUE_V2) batch.baseOffset else batch.lastOffset

      // 判断偏移量是否是递增的
      if (lastOffset >= batch.lastOffset)
        monotonic = false

      // update the last offset seen
      lastOffset = batch.lastOffset

      // 检修消息的大小是否是有效的
      val batchSize = batch.sizeInBytes
      if (batchSize > config.maxMessageSize) {
        brokerTopicStats.topicStats(topicPartition.topic).bytesRejectedRate.mark(records.sizeInBytes)
        brokerTopicStats.allTopicsStats.bytesRejectedRate.mark(records.sizeInBytes)
        throw new RecordTooLargeException(s"The record batch size is $batchSize bytes which exceeds the maximum configured " +
          s"value of ${config.maxMessageSize}.")
      }

      // 通过检查CRC判断消息的有效性
      batch.ensureValid()

      if (batch.maxTimestamp > maxTimestamp) {
        maxTimestamp = batch.maxTimestamp // 最大的时间戳
        offsetOfMaxTimestamp = lastOffset // 最大时间戳的偏移量
      }

      shallowMessageCount += 1
      validBytesCount += batchSize //

      val messageCodec = CompressionCodec.getCompressionCodec(batch.compressionType.id)
      if (messageCodec != NoCompressionCodec) // 源压缩编码
        sourceCodec = messageCodec
    }

    // 服务器希望的编码
    val targetCodec = BrokerCompressionCodec.getTargetCompressionCodec(config.compressionType, sourceCodec)
    LogAppendInfo(firstOffset, lastOffset, maxTimestamp, offsetOfMaxTimestamp, RecordBatch.NO_TIMESTAMP, logStartOffset,
      RecordsProcessingStats.EMPTY, sourceCodec, targetCodec, shallowMessageCount, validBytesCount, monotonic)
  }
  // 更新生产者状态
  private def updateProducers(batch: RecordBatch,
                              producers: mutable.Map[Long, ProducerAppendInfo],
                              isFromClient: Boolean): Option[CompletedTxn] = {
    val producerId = batch.producerId
    // 添加新的ProducerAppendInfo
    val appendInfo = producers.getOrElseUpdate(producerId, producerStateManager.prepareUpdate(producerId, isFromClient))
    appendInfo.append(batch)
  }

  /**
   * Trim any invalid bytes from the end of this message set (if there are any)
   *
   * @param records The records to trim
   * @param info The general information of the message set
   * @return A trimmed message set. This may be the same as what was passed in or it may not.
   */
  private def trimInvalidBytes(records: MemoryRecords, info: LogAppendInfo): MemoryRecords = {
    val validBytes = info.validBytes
    if (validBytes < 0)
      throw new CorruptRecordException("Illegal length of message set " + validBytes + " Message set cannot be appended to log. Possible causes are corrupted produce requests")
    if (validBytes == records.sizeInBytes) { // 正常
      records
    } else {
      // 去除无效字节
      val validByteBuffer = records.buffer.duplicate()
      validByteBuffer.limit(validBytes)
      MemoryRecords.readableRecords(validByteBuffer)
    }
  }

  private[log] def readUncommitted(startOffset: Long, maxLength: Int, maxOffset: Option[Long] = None,
                                   minOneMessage: Boolean = false): FetchDataInfo = {
    read(startOffset, maxLength, maxOffset, minOneMessage, isolationLevel = IsolationLevel.READ_UNCOMMITTED)
  }

  /**
   * 从日志中读取消息。
   *
   * @param startOffset 开始读取的偏移量
   * @param maxLength 要读取的最大字节数
   * @param maxOffset 要读取的偏移量，排他性。 （即，这个偏移不包括在结果消息集中）
   * @param minOneMessage 如果这是真的，即使超过maxLength（如果存在），第一条消息也会被返回。
   * @param isolationLevel 提取器的隔离级别。
    *                       READ_UNCOMMITTED隔离级别具有传统的读取语义（例如，消费者限于获取高水位）。
    *                       在READ_COMMITTED中，消费者仅限于获取最后一个稳定的偏移量。
    *                       此外，在READ_COMMITTED中，事务索引在读取后被查询，以在收集范围中收集被中止的事务的列表，
    *                       在消费者返回给用户之前用它来过滤获取的记录。 请注意，追随者提取总是使用READ_UNCOMMITTED。
   *
   * @throws OffsetOutOfRangeException If startOffset is beyond the log end offset or before the log start offset
   * @return The fetch data information including fetch starting offset metadata and messages read.
   */
  def read(startOffset: Long, maxLength: Int, maxOffset: Option[Long] = None, minOneMessage: Boolean = false,
           isolationLevel: IsolationLevel): FetchDataInfo = {
    maybeHandleIOException(s"Exception while reading from $topicPartition in dir ${dir.getParent}") {
      trace("Reading %d bytes from offset %d in log %s of length %d bytes".format(maxLength, startOffset, name, size))

      // 因为我们不使用锁读取，所以同步有点棘手。我们创建局部变量以避免更新日志的竞争条件。
      val currentNextOffsetMetadata = nextOffsetMetadata
      val next = currentNextOffsetMetadata.messageOffset
      if (startOffset == next) { // 表示未获取到新的数据
        val abortedTransactions =
          if (isolationLevel == IsolationLevel.READ_COMMITTED) Some(List.empty[AbortedTransaction])
          else None
        return FetchDataInfo(currentNextOffsetMetadata, MemoryRecords.EMPTY, firstEntryIncomplete = false,
          abortedTransactions = abortedTransactions)
      }

      var segmentEntry = segments.floorEntry(startOffset)// 开始偏移量的所在的段

      // 尝试读取日志结束偏移量之外的错误时返回错误，或者读取日志起始偏移量以下
      if (startOffset > next || segmentEntry == null || startOffset < logStartOffset)
        throw new OffsetOutOfRangeException("Request for offset %d but we only have log segments in the range %d to %d.".format(startOffset, logStartOffset, next))

      // Do the read on the segment with a base offset less than the target offset
      // but if that segment doesn't contain any messages with an offset greater than that
      // continue to read from successive segments until we get some messages or we reach the end of the log
      while (segmentEntry != null) {
        val segment = segmentEntry.getValue

        // 如果在活动段上发生提取，则可能会出现竞争情况，即在附加消息之后但在更新nextOffsetMetadata之前出现两个提取请求。
        // 在这种情况下，第二次读取可能会导致OffsetOutOfRangeException。
        // 为了解决这个问题，我们将读数限制在暴露位置，而不是活动段的日志末尾。
        val maxPosition = {
          if (segmentEntry == segments.lastEntry) {
            val exposedPos = nextOffsetMetadata.relativePositionInSegment.toLong
            // 如果新分段刚刚推出，请再次检查分段。
            if (segmentEntry != segments.lastEntry)
            // 新的日志段已经推出，我们可以读到文件末尾。
              segment.size
            else
              exposedPos
          } else {
            segment.size
          }
        }
        val fetchInfo = segment.read(startOffset, maxOffset, maxLength, maxPosition, minOneMessage)
        if (fetchInfo == null) {
          segmentEntry = segments.higherEntry(segmentEntry.getKey)
        } else {
          return isolationLevel match {
            case IsolationLevel.READ_UNCOMMITTED => fetchInfo
            case IsolationLevel.READ_COMMITTED => addAbortedTransactions(startOffset, segmentEntry, fetchInfo)
          }
        }
      }

      // okay we are beyond the end of the last segment with no data fetched although the start offset is in range,
      // this can happen when all messages with offset larger than start offsets have been deleted.
      // In this case, we will return the empty set with log end offset metadata
      FetchDataInfo(nextOffsetMetadata, MemoryRecords.EMPTY)
    }
  }

  //  获取区间内的中断事务
  private[log] def collectAbortedTransactions(startOffset: Long, upperBoundOffset: Long): List[AbortedTxn] = {
    val segmentEntry = segments.floorEntry(startOffset)
    val allAbortedTxns = ListBuffer.empty[AbortedTxn]
    def accumulator(abortedTxns: List[AbortedTxn]): Unit = allAbortedTxns ++= abortedTxns
    collectAbortedTransactions(logStartOffset, upperBoundOffset, segmentEntry, accumulator)
    allAbortedTxns.toList
  }

  // 向FetchDataInfo添加中断事务
  private def addAbortedTransactions(startOffset: Long, segmentEntry: JEntry[JLong, LogSegment],
                                     fetchInfo: FetchDataInfo): FetchDataInfo = {
    val fetchSize = fetchInfo.records.sizeInBytes
    val startOffsetPosition = OffsetPosition(fetchInfo.fetchOffsetMetadata.messageOffset,
      fetchInfo.fetchOffsetMetadata.relativePositionInSegment)
    val upperBoundOffset = segmentEntry.getValue.fetchUpperBoundOffset(startOffsetPosition, fetchSize).getOrElse {
      val nextSegmentEntry = segments.higherEntry(segmentEntry.getKey) // 段末尾
      if (nextSegmentEntry != null)
        nextSegmentEntry.getValue.baseOffset
      else
        logEndOffset
    }

    // 收集中断事务
    val abortedTransactions = ListBuffer.empty[AbortedTransaction]
    def accumulator(abortedTxns: List[AbortedTxn]): Unit = abortedTransactions ++= abortedTxns.map(_.asAbortedTransaction)
    collectAbortedTransactions(startOffset, upperBoundOffset, segmentEntry, accumulator)

    FetchDataInfo(fetchOffsetMetadata = fetchInfo.fetchOffsetMetadata,
      records = fetchInfo.records,
      firstEntryIncomplete = fetchInfo.firstEntryIncomplete,
      abortedTransactions = Some(abortedTransactions.toList))
  }

  private def collectAbortedTransactions(startOffset: Long, upperBoundOffset: Long,
                                         startingSegmentEntry: JEntry[JLong, LogSegment],
                                         accumulator: List[AbortedTxn] => Unit): Unit = {
    var segmentEntry = startingSegmentEntry// 起始的segment
    while (segmentEntry != null) {
      val searchResult = segmentEntry.getValue.collectAbortedTxns(startOffset, upperBoundOffset)
      accumulator(searchResult.abortedTransactions)// 进行收集，直到收集的完成事务
      if (searchResult.isComplete)
        return
      segmentEntry = segments.higherEntry(segmentEntry.getKey)
    }
  }

  /**
   * Get an offset based on the given timestamp
   * The offset returned is the offset of the first message whose timestamp is greater than or equals to the
   * given timestamp.
   *
   * If no such message is found, the log end offset is returned.
   *
   * `NOTE:` OffsetRequest V0 does not use this method, the behavior of OffsetRequest V0 remains the same as before
   * , i.e. it only gives back the timestamp based on the last modification time of the log segments.
   *
   * @param targetTimestamp The given timestamp for offset fetching.
   * @return The offset of the first message whose timestamp is greater than or equals to the given timestamp.
   *         None if no such message is found.
   */
  def fetchOffsetsByTimestamp(targetTimestamp: Long): Option[TimestampOffset] = {
    maybeHandleIOException(s"Error while fetching offset by timestamp for $topicPartition in dir ${dir.getParent}") {
      debug(s"Searching offset for timestamp $targetTimestamp")

      if (config.messageFormatVersion < KAFKA_0_10_0_IV0 &&
        targetTimestamp != ListOffsetRequest.EARLIEST_TIMESTAMP &&
        targetTimestamp != ListOffsetRequest.LATEST_TIMESTAMP)
        throw new UnsupportedForMessageFormatException(s"Cannot search offsets based on timestamp because message format version " +
          s"for partition $topicPartition is ${config.messageFormatVersion} which is earlier than the minimum " +
          s"required version $KAFKA_0_10_0_IV0")


      // 缓存以避免竞争条件。 `toBuffer`比大多数替代方法更快，并且提供了持续的时间访问，同时可以安全地使用并发集合，而不像`toArray`。
      val segmentsCopy = logSegments.toBuffer
      // 对于最早的和最新的，我们不需要返回时间戳。
      if (targetTimestamp == ListOffsetRequest.EARLIEST_TIMESTAMP)
        return Some(TimestampOffset(RecordBatch.NO_TIMESTAMP, logStartOffset))
      else if (targetTimestamp == ListOffsetRequest.LATEST_TIMESTAMP)
        return Some(TimestampOffset(RecordBatch.NO_TIMESTAMP, logEndOffset))

      val targetSeg = {
        // 获取所有最大时间戳小于目标时间戳的段
        val earlierSegs = segmentsCopy.takeWhile(_.largestTimestamp < targetTimestamp)
        // 我们需要搜索最大时间戳大于目标时间戳的第一个段（如果有的话）。
        if (earlierSegs.length < segmentsCopy.length)
          Some(segmentsCopy(earlierSegs.length))
        else
          None
      }

      targetSeg.flatMap(_.findOffsetByTimestamp(targetTimestamp, logStartOffset))
    }
  }

  /**
   * Given a message offset, find its corresponding offset metadata in the log.
   * If the message offset is out of range, return unknown offset metadata
   */
  def convertToOffsetMetadata(offset: Long): LogOffsetMetadata = {
    try {
      val fetchDataInfo = readUncommitted(offset, 1)
      fetchDataInfo.fetchOffsetMetadata
    } catch {
      case _: OffsetOutOfRangeException => LogOffsetMetadata.UnknownOffsetMetadata
    }
  }

  /**
   * Delete any log segments matching the given predicate function,
   * starting with the oldest segment and moving forward until a segment doesn't match.
   *
   * @param predicate A function that takes in a candidate log segment and the next higher segment
   *                  (if there is one) and returns true iff it is deletable
   * @return The number of segments deleted
   */
  private def deleteOldSegments(predicate: (LogSegment, Option[LogSegment]) => Boolean, reason: String): Int = {
    lock synchronized {
      val deletable = deletableSegments(predicate)
      if (deletable.nonEmpty)
        info(s"Found deletable segments with base offsets [${deletable.map(_.baseOffset).mkString(",")}] due to $reason")
      deleteSegments(deletable)
    }
  }

  private def deleteSegments(deletable: Iterable[LogSegment]): Int = {
    maybeHandleIOException(s"Error while deleting segments for $topicPartition in dir ${dir.getParent}") {
      val numToDelete = deletable.size
      if (numToDelete > 0) {
        // we must always have at least one segment, so if we are going to delete all the segments, create a new one first
        if (segments.size == numToDelete)
          roll()
        lock synchronized {
          checkIfMemoryMappedBufferClosed()
          // remove the segments for lookups
          deletable.foreach(deleteSegment)
          maybeIncrementLogStartOffset(segments.firstEntry.getValue.baseOffset)
        }
      }
      numToDelete
    }
  }

  /**
   * Find segments starting from the oldest until the user-supplied predicate is false or the segment
   * containing the current high watermark is reached. We do not delete segments with offsets at or beyond
   * the high watermark to ensure that the log start offset can never exceed it. If the high watermark
   * has not yet been initialized, no segments are eligible for deletion.
   *
   * A final segment that is empty will never be returned (since we would just end up re-creating it).
   *
   * @param predicate A function that takes in a candidate log segment and the next higher segment
   *                  (if there is one) and returns true iff it is deletable
   * @return the segments ready to be deleted
   */
  private def deletableSegments(predicate: (LogSegment, Option[LogSegment]) => Boolean): Iterable[LogSegment] = {
    if (segments.isEmpty || replicaHighWatermark.isEmpty) {
      Seq.empty
    } else {
      val highWatermark = replicaHighWatermark.get
      val deletable = ArrayBuffer.empty[LogSegment]
      var segmentEntry = segments.firstEntry
      while (segmentEntry != null) {
        val segment = segmentEntry.getValue
        val nextSegmentEntry = segments.higherEntry(segmentEntry.getKey)
        val (nextSegment, upperBoundOffset, isLastSegmentAndEmpty) = if (nextSegmentEntry != null)
          (nextSegmentEntry.getValue, nextSegmentEntry.getValue.baseOffset, false)
        else
          (null, logEndOffset, segment.size == 0)

        if (highWatermark >= upperBoundOffset && predicate(segment, Option(nextSegment)) && !isLastSegmentAndEmpty) {
          deletable += segment
          segmentEntry = nextSegmentEntry
        } else {
          segmentEntry = null
        }
      }
      deletable
    }
  }

  /**
   * Delete any log segments that have either expired due to time based retention
   * or because the log size is > retentionSize
   */
  def deleteOldSegments(): Int = {
    if (!config.delete) return 0
    deleteRetentionMsBreachedSegments() + deleteRetentionSizeBreachedSegments() + deleteLogStartOffsetBreachedSegments()
  }

  private def deleteRetentionMsBreachedSegments(): Int = {
    if (config.retentionMs < 0) return 0
    val startMs = time.milliseconds
    deleteOldSegments((segment, _) => startMs - segment.largestTimestamp > config.retentionMs,
      reason = s"retention time ${config.retentionMs}ms breach")
  }

  private def deleteRetentionSizeBreachedSegments(): Int = {
    if (config.retentionSize < 0 || size < config.retentionSize) return 0
    var diff = size - config.retentionSize
    def shouldDelete(segment: LogSegment, nextSegmentOpt: Option[LogSegment]) = {
      if (diff - segment.size >= 0) {
        diff -= segment.size
        true
      } else {
        false
      }
    }

    deleteOldSegments(shouldDelete, reason = s"retention size in bytes ${config.retentionSize} breach")
  }

  private def deleteLogStartOffsetBreachedSegments(): Int = {
    def shouldDelete(segment: LogSegment, nextSegmentOpt: Option[LogSegment]) =
      nextSegmentOpt.exists(_.baseOffset <= logStartOffset)

    deleteOldSegments(shouldDelete, reason = s"log start offset $logStartOffset breach")
  }

  def isFuture: Boolean = dir.getName.endsWith(Log.FutureDirSuffix)

  /**
   * The size of the log in bytes
   */
  def size: Long = Log.sizeInBytes(logSegments)

  /**
   * 将附加到日志的下一个消息的偏移元数据。
   */
  def logEndOffsetMetadata: LogOffsetMetadata = nextOffsetMetadata

  /**
   * 下一条消息将被附加到日志的偏移量
   */
  def logEndOffset: Long = nextOffsetMetadata.messageOffset

  /**
   * Roll the log over to a new empty log segment if necessary.
   *
   * @param messagesSize The messages set size in bytes
   * @param maxTimestampInMessages The maximum timestamp in the messages.
   * logSegment will be rolled if one of the following conditions met
   * <ol>
   * <li> The logSegment is full
   * <li> The maxTime has elapsed since the timestamp of first message in the segment (or since the create time if
   * the first message does not have a timestamp)
   * <li> The index is full
   * </ol>
   * @return The currently active segment after (perhaps) rolling to a new segment
   */
  private def maybeRoll(messagesSize: Int, maxTimestampInMessages: Long, maxOffsetInMessages: Long): LogSegment = {
    val segment = activeSegment // 当前段
    val now = time.milliseconds
    if (segment.shouldRoll(messagesSize, maxTimestampInMessages, maxOffsetInMessages, now)) { // 需要回滚
      debug(s"Rolling new log segment in $name (log_size = ${segment.size}/${config.segmentSize}}, " +
          s"offset_index_size = ${segment.offsetIndex.entries}/${segment.offsetIndex.maxEntries}, " +
          s"time_index_size = ${segment.timeIndex.entries}/${segment.timeIndex.maxEntries}, " +
          s"inactive_time_ms = ${segment.timeWaitedForRoll(now, maxTimestampInMessages)}/${config.segmentMs - segment.rollJitterMs}).")
      /*
        maxOffsetInMessages - Integer.MAX_VALUE是消息集合中第一个偏移量的启发式值。
        由于消息中的偏移量相差不会超过Integer.MAX_VALUE，因此可以保证<=集合中的实际第一偏移量。
        确定集合中真正的第一偏移量需要解压缩，追随者在日志追加期间试图避免解压缩。
        先前的行为从旧段中分配了新的baseOffset = logEndOffset。
        在两个连续的消息在Integer.MAX_VALUE.toLong + 2或更多的偏移量上不同的情况下，这是有问题的。
        在这种情况下，先前的行为会滚动一个新的日志段，其基本偏移量太低而不能包含下一个消息。
        当副本正在从头开始恢复高度压缩的主题时，这种边缘情况是可能的。
       */
      roll(maxOffsetInMessages - Integer.MAX_VALUE)
    } else {
      segment
    }
  }

  /**
   * 将日志转移到以当前logEndOffset开始的新活动段。
   * 这会将索引修剪为当前包含的条目数量的确切大小。
   *
   * @return The newly rolled segment
   */
  def roll(expectedNextOffset: Long = 0): LogSegment = {
    maybeHandleIOException(s"Error while rolling log segment for $topicPartition in dir ${dir.getParent}") {
      val start = time.hiResClockMs()
      lock synchronized {
        checkIfMemoryMappedBufferClosed()
        val newOffset = math.max(expectedNextOffset, logEndOffset)
        // 为创建新的段，做准备
        val logFile = Log.logFile(dir, newOffset)
        val offsetIdxFile = offsetIndexFile(dir, newOffset)
        val timeIdxFile = timeIndexFile(dir, newOffset)
        val txnIdxFile = transactionIndexFile(dir, newOffset)
        for (file <- List(logFile, offsetIdxFile, timeIdxFile, txnIdxFile) if file.exists) {
          warn(s"Newly rolled segment file ${file.getAbsolutePath} already exists; deleting it first")
          Files.delete(file.toPath)
        }

        Option(segments.lastEntry).foreach(_.getValue.onBecomeInactiveSegment())

        // take a snapshot of the producer state to facilitate recovery. It is useful to have the snapshot
        // offset align with the new segment offset since this ensures we can recover the segment by beginning
        // with the corresponding snapshot file and scanning the segment data. Because the segment base offset
        // may actually be ahead of the current producer state end offset (which corresponds to the log end offset),
        // we manually override the state offset here prior to taking the snapshot.
        producerStateManager.updateMapEndOffset(newOffset)
        producerStateManager.takeSnapshot()

        // 设置新的段
        val segment = LogSegment.open(dir,
          baseOffset = newOffset,
          config,
          time = time,
          fileAlreadyExists = false,
          initFileSize = initFileSize,
          preallocate = config.preallocate)
        val prev = addSegment(segment)
        if (prev != null)
          throw new KafkaException("Trying to roll a new log segment for topic partition %s with start offset %d while it already exists.".format(name, newOffset))
        // We need to update the segment base offset and append position data of the metadata when log rolls.
        // The next offset should not change.
        updateLogEndOffset(nextOffsetMetadata.messageOffset)
        // schedule an asynchronous flush of the old segment
        scheduler.schedule("flush-log", () => flush(newOffset), delay = 0L)

        info(s"Rolled new log segment for '$name' in ${time.hiResClockMs() - start} ms.")

        segment
      }
    }
  }

  /**
   * 自上次刷新后附加到日志的消息数量
   */
  def unflushedMessages() = this.logEndOffset - this.recoveryPoint

  /**
   * Flush all log segments
   */
  def flush(): Unit = flush(this.logEndOffset)

  /**
   * 刷新所有偏移量的日志段直至偏移-1
   *
   * @param offset The offset to flush up to (non-inclusive); the new recovery point
   */
  def flush(offset: Long) : Unit = {
    maybeHandleIOException(s"Error while flushing log for $topicPartition in dir ${dir.getParent} with offset $offset") {
      if (offset <= this.recoveryPoint)
        return
      debug("Flushing log '" + name + " up to offset " + offset + ", last flushed: " + lastFlushTime + " current time: " +
        time.milliseconds + " unflushed = " + unflushedMessages)
      for (segment <- logSegments(this.recoveryPoint, offset))
        segment.flush()

      lock synchronized {
        checkIfMemoryMappedBufferClosed()
        if (offset > this.recoveryPoint) {
          this.recoveryPoint = offset
          lastFlushedTime.set(time.milliseconds)
        }
      }
    }
  }

  /**
   * Cleanup old producer snapshots after the recovery point is checkpointed. It is useful to retain
   * the snapshots from the recent segments in case we need to truncate and rebuild the producer state.
   * Otherwise, we would always need to rebuild from the earliest segment.
   *
   * More specifically:
   *
   * 1. We always retain the producer snapshot from the last two segments. This solves the common case
   * of truncating to an offset within the active segment, and the rarer case of truncating to the previous segment.
   *
   * 2. We only delete snapshots for offsets less than the recovery point. The recovery point is checkpointed
   * periodically and it can be behind after a hard shutdown. Since recovery starts from the recovery point, the logic
   * of rebuilding the producer snapshots in one pass and without loading older segments is simpler if we always
   * have a producer snapshot for all segments being recovered.
   *
   * Return the minimum snapshots offset that was retained.
   */
  def deleteSnapshotsAfterRecoveryPointCheckpoint(): Long = {
    val minOffsetToRetain = minSnapshotsOffsetToRetain
    producerStateManager.deleteSnapshotsBefore(minOffsetToRetain)
    minOffsetToRetain
  }

  // Visible for testing, see `deleteSnapshotsAfterRecoveryPointCheckpoint()` for details
  private[log] def minSnapshotsOffsetToRetain: Long = {
    lock synchronized {
      val twoSegmentsMinOffset = lowerSegment(activeSegment.baseOffset).getOrElse(activeSegment).baseOffset
      // Prefer segment base offset
      val recoveryPointOffset = lowerSegment(recoveryPoint).map(_.baseOffset).getOrElse(recoveryPoint)
      math.min(recoveryPointOffset, twoSegmentsMinOffset)
    }
  }

  private def lowerSegment(offset: Long): Option[LogSegment] =
    Option(segments.lowerEntry(offset)).map(_.getValue)

  /**
   * Completely delete this log directory and all contents from the file system with no delay
   */
  private[log] def delete() {
    maybeHandleIOException(s"Error while deleting log for $topicPartition in dir ${dir.getParent}") {
      lock synchronized {
        checkIfMemoryMappedBufferClosed()
        removeLogMetrics()
        logSegments.foreach(_.deleteIfExists())
        segments.clear()
        _leaderEpochCache.clear()
        Utils.delete(dir)
        // File handlers will be closed if this log is deleted
        isMemoryMappedBufferClosed = true
      }
    }
  }

  // visible for testing
  private[log] def takeProducerSnapshot(): Unit = lock synchronized {
    checkIfMemoryMappedBufferClosed()
    producerStateManager.takeSnapshot()
  }

  // visible for testing
  private[log] def latestProducerSnapshotOffset: Option[Long] = lock synchronized {
    producerStateManager.latestSnapshotOffset
  }

  // visible for testing
  private[log] def oldestProducerSnapshotOffset: Option[Long] = lock synchronized {
    producerStateManager.oldestSnapshotOffset
  }

  // visible for testing
  private[log] def latestProducerStateEndOffset: Long = lock synchronized {
    producerStateManager.mapEndOffset
  }

  /**
   * 截断此日志，使其以最大偏移 < 目标偏移量结束。
   *
   * @param targetOffset The offset to truncate to, an upper bound on all offsets in the log after truncation is complete.
   * @return True iff targetOffset < logEndOffset
   */
  private[log] def truncateTo(targetOffset: Long): Boolean = {
    maybeHandleIOException(s"Error while truncating log to offset $targetOffset for $topicPartition in dir ${dir.getParent}") {
      if (targetOffset < 0)
        throw new IllegalArgumentException("Cannot truncate to a negative offset (%d).".format(targetOffset))
      if (targetOffset >= logEndOffset) {
        info("Truncating %s to %d has no effect as the largest offset in the log is %d.".format(name, targetOffset, logEndOffset - 1))
        false
      } else {
        info("Truncating log %s to offset %d.".format(name, targetOffset))
        lock synchronized {
          checkIfMemoryMappedBufferClosed()
          if (segments.firstEntry.getValue.baseOffset > targetOffset) { // 目标偏移量小于起始segments的起始偏移量
            truncateFullyAndStartAt(targetOffset) // 截断全部的segment
          } else {
            val deletable = logSegments.filter(segment => segment.baseOffset > targetOffset) // 待删除的segment
            deletable.foreach(deleteSegment)
            activeSegment.truncateTo(targetOffset)
            updateLogEndOffset(targetOffset)
            this.recoveryPoint = math.min(targetOffset, this.recoveryPoint)
            this.logStartOffset = math.min(targetOffset, this.logStartOffset)
            _leaderEpochCache.clearAndFlushLatest(targetOffset)
            loadProducerState(targetOffset, reloadFromCleanShutdown = false)
          }
          true
        }
      }
    }
  }

  /**
   *  删除日志中的所有数据并从新的偏移量开始
   *
   *  @param newOffset The new offset to start the log with
   */
  private[log] def truncateFullyAndStartAt(newOffset: Long) {
    maybeHandleIOException(s"Error while truncating the entire log for $topicPartition in dir ${dir.getParent}") {
      debug(s"Truncate and start log '$name' at offset $newOffset")
      lock synchronized {
        checkIfMemoryMappedBufferClosed()
        val segmentsToDelete = logSegments.toList
        segmentsToDelete.foreach(deleteSegment) // 删除所有的segment
        addSegment(LogSegment.open(dir, // 添加新的分区
          baseOffset = newOffset,
          config = config,
          time = time,
          fileAlreadyExists = false,
          initFileSize = initFileSize,
          preallocate = config.preallocate))
        updateLogEndOffset(newOffset)// 更新偏移量
        _leaderEpochCache.clearAndFlush()

        producerStateManager.truncate()
        producerStateManager.updateMapEndOffset(newOffset)
        updateFirstUnstableOffset()

        this.recoveryPoint = math.min(newOffset, this.recoveryPoint)
        this.logStartOffset = newOffset
      }
    }
  }

  /**
   * The time this log is last known to have been fully flushed to disk
   */
  def lastFlushTime: Long = lastFlushedTime.get

  /**
   * 当前正在追加的活动段
   */
  def activeSegment = segments.lastEntry.getValue

  /**
   * All the log segments in this log ordered from oldest to newest
   */
  def logSegments: Iterable[LogSegment] = segments.values.asScala

  /**
   * 获取以包含“from”开头的段开头，并以包含最多“to-1”或日志结尾的段结束（如果要> logEndOffset）
   */
  def logSegments(from: Long, to: Long): Iterable[LogSegment] = {
    lock synchronized {
      val view = Option(segments.floorKey(from)).map { floor =>
        segments.subMap(floor, to)
      }.getOrElse(segments.headMap(to))
      view.values.asScala
    }
  }

  override def toString = "Log(" + dir + ")"

  /**
   * This method performs an asynchronous log segment delete by doing the following:
   * <ol>
   *   <li>It removes the segment from the segment map so that it will no longer be used for reads.
   *   <li>It renames the index and log files by appending .deleted to the respective file name
   *   <li>It schedules an asynchronous delete operation to occur in the future
   * </ol>
   * This allows reads to happen concurrently without synchronization and without the possibility of physically
   * deleting a file while it is being read from.
   *
   * This method does not need to convert IOException to KafkaStorageException because it is either called before all logs are loaded
   * or the immediate caller will catch and handle IOException
   *
   * @param segment The log segment to schedule for deletion
   */
  private def deleteSegment(segment: LogSegment) {
    info(s"Scheduling log segment [baseOffset ${segment.baseOffset}, size ${segment.size}] for log $name for deletion.")
    lock synchronized {
      segments.remove(segment.baseOffset)// 从segments中移除
      asyncDeleteSegment(segment)
    }
  }

  /**
   * 对给定文件执行异步删除（否则不执行任何操作）
   *
   * This method does not need to convert IOException (thrown from changeFileSuffixes) to KafkaStorageException because
   * it is either called before all logs are loaded or the caller will catch and handle IOException
   *
   * @throws IOException if the file can't be renamed and still exists
   */
  private def asyncDeleteSegment(segment: LogSegment) {
    segment.changeFileSuffixes("", Log.DeletedFileSuffix)// 设置删除的后置
    def deleteSeg() {
      info(s"Deleting segment ${segment.baseOffset} from log $name.")
      maybeHandleIOException(s"Error while deleting segments for $topicPartition in dir ${dir.getParent}") {
        segment.deleteIfExists()
      }
    }
    scheduler.schedule("delete-file", deleteSeg _, delay = config.fileDeleteDelayMs)// 定时执行删除任务
  }

  /**
   * 交换一个新的段并以安全方式删除一个或多个现有段。 旧的细分将被异步删除。
   * This method does not need to convert IOException to KafkaStorageException because it is either called before all logs are loaded
   * or the caller will catch and handle IOException
   *
   * The sequence of operations is:
   * <ol>
   *   <li> Cleaner creates new segment with suffix .cleaned and invokes replaceSegments().
   *        If broker crashes at this point, the clean-and-swap operation is aborted and
   *        the .cleaned file is deleted on recovery in loadSegments().
   *   <li> New segment is renamed .swap. If the broker crashes after this point before the whole
   *        operation is completed, the swap operation is resumed on recovery as described in the next step.
   *   <li> Old segment files are renamed to .deleted and asynchronous delete is scheduled.
   *        If the broker crashes, any .deleted files left behind are deleted on recovery in loadSegments().
   *        replaceSegments() is then invoked to complete the swap with newSegment recreated from
   *        the .swap file and oldSegments containing segments which were not renamed before the crash.
   *   <li> Swap segment is renamed to replace the existing segment, completing this operation.
   *        If the broker crashes, any .deleted files which may be left behind are deleted
   *        on recovery in loadSegments().
   * </ol>
   *
   * @param newSegment The new log segment to add to the log
   * @param oldSegments The old log segments to delete from the log
   * @param isRecoveredSwapFile true if the new segment was created from a swap file during recovery after a crash
   */
  private[log] def replaceSegments(newSegment: LogSegment, oldSegments: Seq[LogSegment], isRecoveredSwapFile: Boolean = false) {
    lock synchronized {
      checkIfMemoryMappedBufferClosed()// 检查是否关闭
      // need to do this in two phases to be crash safe AND do the delete asynchronously
      // if we crash in the middle of this we complete the swap in loadSegments()
      if (!isRecoveredSwapFile)
        newSegment.changeFileSuffixes(Log.CleanedFileSuffix, Log.SwapFileSuffix)
      addSegment(newSegment)

      // delete the old files
      for (seg <- oldSegments) {// 移除并删除旧的segment
        // remove the index entry
        if (seg.baseOffset != newSegment.baseOffset)
          segments.remove(seg.baseOffset)
        // delete segment
        asyncDeleteSegment(seg)
      }
      // 我们现在很安全，删除交换后缀
      newSegment.changeFileSuffixes(Log.SwapFileSuffix, "")
    }
  }

  /**
   * remove deleted log metrics
   */
  private[log] def removeLogMetrics(): Unit = {
    removeMetric("NumLogSegments", tags)
    removeMetric("LogStartOffset", tags)
    removeMetric("LogEndOffset", tags)
    removeMetric("Size", tags)
  }
  /**
   * Add the given segment to the segments in this log. If this segment replaces an existing segment, delete it.
   *
   * @param segment The segment to add
   */
  def addSegment(segment: LogSegment) = this.segments.put(segment.baseOffset, segment)

  // 将有io异常的文件添加到logDirFailureChannel
  private def maybeHandleIOException[T](msg: => String)(fun: => T): T = {
    try {
      fun
    } catch {
      case e: IOException =>
        logDirFailureChannel.maybeAddOfflineLogDir(dir.getParent, msg, e)
        throw new KafkaStorageException(msg, e)
    }
  }

}

/**
 * logs的帮助方法
 */
object Log {

  /** a log file */
  val LogFileSuffix = ".log"

  /** an index file */
  val IndexFileSuffix = ".index"

  /** a time index file */
  val TimeIndexFileSuffix = ".timeindex"

  /** an (aborted) txn index */
  val TxnIndexFileSuffix = ".txnindex"

  val ProducerSnapshotFileSuffix = ".snapshot"

  /** 计划删除的文件 */
  val DeletedFileSuffix = ".deleted"

  /** 用于日志清理的临时文件 */
  val CleanedFileSuffix = ".cleaned"

  /** A temporary file used when swapping files into the log */
  val SwapFileSuffix = ".swap"

  /** Clean shutdown file that indicates the broker was cleanly shutdown in 0.8 and higher.
   * This is used to avoid unnecessary recovery after a clean shutdown. In theory this could be
   * avoided by passing in the recovery point, however finding the correct position to do this
   * requires accessing the offset index which may not be safe in an unclean shutdown.
   * For more information see the discussion in PR#2104
   */
  val CleanShutdownFile = ".kafka_cleanshutdown"

  /** 一个计划要删除的目录 */
  val DeleteDirSuffix = "-delete"

  /** 一个用于将来分区的目录 */
  val FutureDirSuffix = "-future"

  private val DeleteDirPattern = Pattern.compile(s"^(\\S+)-(\\S+)\\.(\\S+)$DeleteDirSuffix")
  private val FutureDirPattern = Pattern.compile(s"^(\\S+)-(\\S+)\\.(\\S+)$FutureDirSuffix")

  val UnknownLogStartOffset = -1L

  def apply(dir: File,
            config: LogConfig,
            logStartOffset: Long,
            recoveryPoint: Long,
            scheduler: Scheduler,
            brokerTopicStats: BrokerTopicStats,
            time: Time = Time.SYSTEM,
            maxProducerIdExpirationMs: Int,
            producerIdExpirationCheckIntervalMs: Int,
            logDirFailureChannel: LogDirFailureChannel): Log = {
    val topicPartition = Log.parseTopicPartitionName(dir)
    val producerStateManager = new ProducerStateManager(topicPartition, dir, maxProducerIdExpirationMs)
    new Log(dir, config, logStartOffset, recoveryPoint, scheduler, brokerTopicStats, time, maxProducerIdExpirationMs,
      producerIdExpirationCheckIntervalMs, topicPartition, producerStateManager, logDirFailureChannel)
  }

  /**
   * Make log segment file name from offset bytes. All this does is pad out the offset number with zeros
   * so that ls sorts the files numerically.
   *
   * @param offset The offset to use in the file name
   * @return The filename
   */
  def filenamePrefixFromOffset(offset: Long): String = {
    val nf = NumberFormat.getInstance()
    nf.setMinimumIntegerDigits(20)
    nf.setMaximumFractionDigits(0)
    nf.setGroupingUsed(false)
    nf.format(offset)
  }

  /**
   * Construct a log file name in the given dir with the given base offset and the given suffix
   *
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   * @param suffix The suffix to be appended to the file name (e.g. "", ".deleted", ".cleaned", ".swap", etc.)
   */
  def logFile(dir: File, offset: Long, suffix: String = ""): File =
    new File(dir, filenamePrefixFromOffset(offset) + LogFileSuffix + suffix)

  /**
    * Return a directory name to rename the log directory to for async deletion. The name will be in the following
    * format: topic-partition.uniqueId-delete where topic, partition and uniqueId are variables.
    */
  def logDeleteDirName(topicPartition: TopicPartition): String = {
    logDirNameWithSuffix(topicPartition, DeleteDirSuffix)
  }

  /**
    * Return a future directory name for the given topic partition. The name will be in the following
    * format: topic-partition.uniqueId-future where topic, partition and uniqueId are variables.
    */
  def logFutureDirName(topicPartition: TopicPartition): String = {
    logDirNameWithSuffix(topicPartition, FutureDirSuffix)
  }

  private def logDirNameWithSuffix(topicPartition: TopicPartition, suffix: String): String = {
    val uniqueId = java.util.UUID.randomUUID.toString.replaceAll("-", "")
    s"${logDirName(topicPartition)}.$uniqueId$suffix"
  }

  /**
    * Return a directory name for the given topic partition. The name will be in the following
    * format: topic-partition where topic, partition are variables.
    */
  def logDirName(topicPartition: TopicPartition): String = {
    s"${topicPartition.topic}-${topicPartition.partition}"
  }

  /**
   * 使用给定的基偏移和给定的后缀在给定的DIR中构造索引文件
   *
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   * @param suffix The suffix to be appended to the file name ("", ".deleted", ".cleaned", ".swap", etc.)
   */
  def offsetIndexFile(dir: File, offset: Long, suffix: String = ""): File =
    new File(dir, filenamePrefixFromOffset(offset) + IndexFileSuffix + suffix)

  /**
   * Construct a time index file name in the given dir using the given base offset and the given suffix
   *
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   * @param suffix The suffix to be appended to the file name ("", ".deleted", ".cleaned", ".swap", etc.)
   */
  def timeIndexFile(dir: File, offset: Long, suffix: String = ""): File =
    new File(dir, filenamePrefixFromOffset(offset) + TimeIndexFileSuffix + suffix)

  /**
   * 使用给定的偏移量构造生产者ID快照文件。
   *
   * @param dir The directory in which the log will reside
   * @param offset The last offset (exclusive) included in the snapshot
   */
  def producerSnapshotFile(dir: File, offset: Long): File =
    new File(dir, filenamePrefixFromOffset(offset) + ProducerSnapshotFileSuffix)

  /**
   * Construct a transaction index file name in the given dir using the given base offset and the given suffix
   *
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   * @param suffix The suffix to be appended to the file name ("", ".deleted", ".cleaned", ".swap", etc.)
   */
  def transactionIndexFile(dir: File, offset: Long, suffix: String = ""): File =
    new File(dir, filenamePrefixFromOffset(offset) + TxnIndexFileSuffix + suffix)

  // 从文件名中获取偏移量
  def offsetFromFile(file: File): Long = {
    val filename = file.getName
    filename.substring(0, filename.indexOf('.')).toLong
  }

  /**
    * Calculate a log's size (in bytes) based on its log segments
    *
    * @param segments The log segments to calculate the size of
    * @return Sum of the log segments' sizes (in bytes)
    */
  def sizeInBytes(segments: Iterable[LogSegment]): Long =
    segments.map(_.size.toLong).sum

  /**
   * 解析日志的目录名称中的主题和分区
   */
  def parseTopicPartitionName(dir: File): TopicPartition = {
    if (dir == null)
      throw new KafkaException("dir should not be null")

    def exception(dir: File): KafkaException = {
      new KafkaException(s"Found directory ${dir.getCanonicalPath}, '${dir.getName}' is not in the form of " +
        "topic-partition or topic-partition.uniqueId-delete (if marked for deletion).\n" +
        "Kafka's log directories (and children) should only contain Kafka topic data.")
    }

    val dirName = dir.getName
    if (dirName == null || dirName.isEmpty || !dirName.contains('-'))
      throw exception(dir)
    if (dirName.endsWith(DeleteDirSuffix) && !DeleteDirPattern.matcher(dirName).matches ||
        dirName.endsWith(FutureDirSuffix) && !FutureDirPattern.matcher(dirName).matches)
      throw exception(dir)

    val name: String =
      if (dirName.endsWith(DeleteDirSuffix) || dirName.endsWith(FutureDirSuffix)) dirName.substring(0, dirName.lastIndexOf('.'))
      else dirName

    val index = name.lastIndexOf('-')
    val topic = name.substring(0, index)
    val partitionString = name.substring(index + 1)
    if (topic.isEmpty || partitionString.isEmpty)
      throw exception(dir)

    val partition =
      try partitionString.toInt
      catch { case _: NumberFormatException => throw exception(dir) }

    new TopicPartition(topic, partition)
  }

  // 判断是否是索引文件
  private def isIndexFile(file: File): Boolean = {
    val filename = file.getName
    filename.endsWith(IndexFileSuffix) || filename.endsWith(TimeIndexFileSuffix) || filename.endsWith(TxnIndexFileSuffix)
  }

  // 日志文件
  private def isLogFile(file: File): Boolean =
    file.getPath.endsWith(LogFileSuffix)

}
