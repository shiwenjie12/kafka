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
import java.nio._
import java.nio.file.Files
import java.util.Date
import java.util.concurrent.TimeUnit

import com.yammer.metrics.core.Gauge
import kafka.common._
import kafka.metrics.KafkaMetricsGroup
import kafka.server.{BrokerReconfigurable, KafkaConfig, LogDirFailureChannel}
import kafka.utils._
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.record.MemoryRecords.RecordFilter
import org.apache.kafka.common.record.MemoryRecords.RecordFilter.BatchRetention

import scala.collection.{Set, mutable}
import scala.collection.JavaConverters._

/**
 * The cleaner is responsible for removing obsolete records from logs which have the "compact" retention strategy.
 * A message with key K and offset O is obsolete if there exists a message with key K and offset O' such that O < O'.
 * cleaner负责从具有“紧凑型”保留策略的日志中删除过时的记录。 如果存在带键K的消息并且偏移O'使得O <O'，则具有键K和偏移量O的消息已经过时。
 * Each log can be thought of being split into two sections of segments: a "clean" section which has previously been cleaned followed by a
 * "dirty" section that has not yet been cleaned. The dirty section is further divided into the "cleanable" section followed by an "uncleanable" section.
 * The uncleanable section is excluded from cleaning. The active log segment is always uncleanable. If there is a
 * compaction lag time set, segments whose largest message timestamp is within the compaction lag time of the cleaning operation are also uncleanable.
 *
 * The cleaning is carried out by a pool of background threads. Each thread chooses the dirtiest log that has the "compact" retention policy
 * and cleans that. The dirtiness of the log is guessed by taking the ratio of bytes in the dirty section of the log to the total bytes in the log.
 *
 * To clean a log the cleaner first builds a mapping of key=>last_offset for the dirty section of the log. See kafka.log.OffsetMap for details of
 * the implementation of the mapping.
 *
 * Once the key=>last_offset map is built, the log is cleaned by recopying each log segment but omitting any key that appears in the offset map with a
 * higher offset than what is found in the segment (i.e. messages with a key that appears in the dirty section of the log).
 *
 * To avoid segments shrinking to very small sizes with repeated cleanings we implement a rule by which if we will merge successive segments when
 * doing a cleaning if their log and index size are less than the maximum log and index size prior to the clean beginning.
 *
 * Cleaned segments are swapped into the log as they become available.
 *
 * One nuance that the cleaner must handle is log truncation. If a log is truncated while it is being cleaned the cleaning of that log is aborted.
 *
 * Messages with null payload are treated as deletes for the purpose of log compaction. This means that they receive special treatment by the cleaner.
 * The cleaner will only retain delete records for a period of time to avoid accumulating space indefinitely. This period of time is configurable on a per-topic
 * basis and is measured from the time the segment enters the clean portion of the log (at which point any prior message with that key has been removed).
 * Delete markers in the clean section of the log that are older than this time will not be retained when log segments are being recopied as part of cleaning.
 *
  * 请注意，清洁是更复杂的幂等/事务生产者能力。以下是要点：
 *
  * 1.为了保持活跃生产者的序列号连续性，我们总是保留来自每个生产商ID的最后一批，即使所有批次的记录都已被删除。
  *   一旦生产商编写了新的批次或由于不活动而过期，该批将被删除。
  * 2. 我们不清除超出最后稳定的偏移量。这确保了由清洁器观察到的所有记录已被确定（即提交或中止）。
  *    特别是，这允许我们使用事务索引提前收集中止的事务。
 * 3. 由中止的事务记录立即被清洁器移除而不考虑记录密钥。
  * 4. 事务标记被保留，直到来自同一事务的所有记录批次被移除，并且足够的时间已经过去，
  *    以合理地确保活动的消费者在到达标记的偏移之前不会消耗来自事务的任何数据。
  *    这遵循相同的逻辑用于墓碑删除。
 *
 * @param initialConfig Initial configuration parameters for the cleaner. Actual config may be dynamically updated.
 * @param logDirs The directories where offset checkpoints reside
 * @param logs The pool of logs
 * @param time A way to control the passage of time
 */
class LogCleaner(initialConfig: CleanerConfig,
                 val logDirs: Seq[File],
                 val logs: Pool[TopicPartition, Log],
                 val logDirFailureChannel: LogDirFailureChannel,
                 time: Time = Time.SYSTEM) extends Logging with KafkaMetricsGroup with BrokerReconfigurable
{

  /* 可以动态更新的日志清理程序配置*/
  @volatile private var config = initialConfig

  /* 用于管理正在清理的分区的状态。 封装专用以允许在测试中访问 */
  private[log] val cleanerManager = new LogCleanerManager(logDirs, logs, logDirFailureChannel)

  /* 用于限制所有清除线程的I/O到用户指定的最大速率的节流阀。*/
  private val throttler = new Throttler(desiredRatePerSec = config.maxIoBytesPerSecond,
                                        checkIntervalMs = 300,
                                        throttleDown = true,
                                        "cleaner-io",
                                        "bytes",
                                        time = time)

  /* the threads */
  private val cleaners = mutable.ArrayBuffer[CleanerThread]()

  /* a metric to track the maximum utilization of any thread's buffer in the last cleaning */
  newGauge("max-buffer-utilization-percent",
           new Gauge[Int] {
             def value: Int = cleaners.map(_.lastStats).map(100 * _.bufferUtilization).max.toInt
           })
  /* a metric to track the recopy rate of each thread's last cleaning */
  newGauge("cleaner-recopy-percent",
           new Gauge[Int] {
             def value: Int = {
               val stats = cleaners.map(_.lastStats)
               val recopyRate = stats.map(_.bytesWritten).sum.toDouble / math.max(stats.map(_.bytesRead).sum, 1)
               (100 * recopyRate).toInt
             }
           })
  /* a metric to track the maximum cleaning time for the last cleaning from each thread */
  newGauge("max-clean-time-secs",
           new Gauge[Int] {
             def value: Int = cleaners.map(_.lastStats).map(_.elapsedSecs).max.toInt
           })

  /**
   * Start the background cleaning
   */
  def startup() {
    info("Starting the log cleaner")
    (0 until config.numThreads).foreach { i =>
      val cleaner = new CleanerThread(i)
      cleaners += cleaner
      cleaner.start()
    }
  }

  /**
   * Stop the background cleaning
   */
  def shutdown() {
    info("Shutting down the log cleaner.")
    cleaners.foreach(_.shutdown())
    cleaners.clear()
  }

  override def reconfigurableConfigs: Set[String] = {
    LogCleaner.ReconfigurableConfigs
  }

  override def validateReconfiguration(newConfig: KafkaConfig): Unit = {
    val newCleanerConfig = LogCleaner.cleanerConfig(newConfig)
    val numThreads = newCleanerConfig.numThreads
    numThreads >= 1 && numThreads >= config.numThreads / 2 && numThreads <= config.numThreads * 2
    val currentThreads = config.numThreads
    if (numThreads <= 0)
      throw new ConfigException(s"Log cleaner threads should be at least 1")
    if (numThreads < currentThreads / 2)
      throw new ConfigException(s"Log cleaner threads cannot be reduced to less than half the current value $currentThreads")
    if (numThreads > currentThreads * 2)
      throw new ConfigException(s"Log cleaner threads cannot be increased to more than double the current value $currentThreads")

  }

  /**
    * Reconfigure log clean config. This simply stops current log cleaners and creates new ones.
    * That ensures that if any of the cleaners had failed, new cleaners are created to match the new config.
    */
  override def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {
    config = LogCleaner.cleanerConfig(newConfig)
    shutdown()
    startup()
  }

  /**
   *  Abort the cleaning of a particular partition, if it's in progress. This call blocks until the cleaning of
   *  the partition is aborted.
   */
  def abortCleaning(topicPartition: TopicPartition) {
    cleanerManager.abortCleaning(topicPartition)
  }

  /**
   * Update checkpoint file, removing topics and partitions that no longer exist
   */
  def updateCheckpoints(dataDir: File) {
    cleanerManager.updateCheckpoints(dataDir, update=None)
  }

  def alterCheckpointDir(topicPartition: TopicPartition, sourceLogDir: File, destLogDir: File): Unit = {
    cleanerManager.alterCheckpointDir(topicPartition, sourceLogDir, destLogDir)
  }

  def handleLogDirFailure(dir: String) {
    cleanerManager.handleLogDirFailure(dir)
  }

  /**
   * Truncate cleaner offset checkpoint for the given partition if its checkpointed offset is larger than the given offset
   */
  def maybeTruncateCheckpoint(dataDir: File, topicPartition: TopicPartition, offset: Long) {
    cleanerManager.maybeTruncateCheckpoint(dataDir, topicPartition, offset)
  }

  /**
   *  Abort the cleaning of a particular partition if it's in progress, and pause any future cleaning of this partition.
   *  This call blocks until the cleaning of the partition is aborted and paused.
   */
  def abortAndPauseCleaning(topicPartition: TopicPartition) {
    cleanerManager.abortAndPauseCleaning(topicPartition)
  }

  /**
   *  Resume the cleaning of a paused partition. This call blocks until the cleaning of a partition is resumed.
   */
  def resumeCleaning(topicPartition: TopicPartition) {
    cleanerManager.resumeCleaning(topicPartition)
  }

  /**
   * For testing, a way to know when work has completed. This method waits until the
   * cleaner has processed up to the given offset on the specified topic/partition
   *
   * @param topicPartition The topic and partition to be cleaned
   * @param offset The first dirty offset that the cleaner doesn't have to clean
   * @param maxWaitMs The maximum time in ms to wait for cleaner
   *
   * @return A boolean indicating whether the work has completed before timeout
   */
  def awaitCleaned(topicPartition: TopicPartition, offset: Long, maxWaitMs: Long = 60000L): Boolean = {
    def isCleaned = cleanerManager.allCleanerCheckpoints.get(topicPartition).fold(false)(_ >= offset)
    var remainingWaitMs = maxWaitMs
    while (!isCleaned && remainingWaitMs > 0) {
      val sleepTime = math.min(100, remainingWaitMs)
      Thread.sleep(sleepTime)
      remainingWaitMs -= sleepTime
    }
    isCleaned
  }

  // Only for testing
  private[kafka] def currentConfig: CleanerConfig = config

  // Only for testing
  private[log] def cleanerCount: Int = cleaners.size

  /**
   * The cleaner threads do the actual log cleaning. Each thread processes does its cleaning repeatedly by
   * choosing the dirtiest log, cleaning it, and then swapping in the cleaned segments.
    * 清洁的线程做实际的日志清理。每个线程处理通过选择最脏的日志，清洗它，然后在清洗的部分中交换来重复清洗。
   */
  private class CleanerThread(threadId: Int)
    extends ShutdownableThread(name = "kafka-log-cleaner-thread-" + threadId, isInterruptible = false) {

    protected override def loggerName = classOf[LogCleaner].getName

    if (config.dedupeBufferSize / config.numThreads > Int.MaxValue) // 忽略多于缓存空间
      warn("Cannot use more than 2G of cleaner buffer space per cleaner thread, ignoring excess buffer space...")

    val cleaner = new Cleaner(id = threadId,
                              offsetMap = new SkimpyOffsetMap(memory = math.min(config.dedupeBufferSize / config.numThreads, Int.MaxValue).toInt,
                                                              hashAlgorithm = config.hashAlgorithm),
                              ioBufferSize = config.ioBufferSize / config.numThreads / 2,
                              maxIoBufferSize = config.maxMessageSize,
                              dupBufferLoadFactor = config.dedupeBufferLoadFactor,
                              throttler = throttler,
                              time = time,
                              checkDone = checkDone)

    @volatile var lastStats: CleanerStats = new CleanerStats()

    private def checkDone(topicPartition: TopicPartition) {
      if (!isRunning)
        throw new ThreadShutdownException
      cleanerManager.checkCleaningAborted(topicPartition)
    }

    /**
     * The main loop for the cleaner thread
     */
    override def doWork() {
      cleanOrSleep()
    }

    /**
     * Clean a log if there is a dirty log available, otherwise sleep for a bit
     */
    private def cleanOrSleep() {
      val cleaned = cleanerManager.grabFilthiestCompactedLog(time) match {// 最脏的数据段
        case None =>
          false
        case Some(cleanable) =>
          // there's a log, clean it
          var endOffset = cleanable.firstDirtyOffset
          try {
            val (nextDirtyOffset, cleanerStats) = cleaner.clean(cleanable)
            recordStats(cleaner.id, cleanable.log.name, cleanable.firstDirtyOffset, endOffset, cleanerStats)
            endOffset = nextDirtyOffset
          } catch {
            case _: LogCleaningAbortedException => // task can be aborted, let it go.
            case _: KafkaStorageException => // partition is already offline. let it go.
            case e: IOException =>
              val msg = s"Failed to clean up log for ${cleanable.topicPartition} in dir ${cleanable.log.dir.getParent} due to IOException"
              logDirFailureChannel.maybeAddOfflineLogDir(cleanable.log.dir.getParent, msg, e)
          } finally {
            cleanerManager.doneCleaning(cleanable.topicPartition, cleanable.log.dir.getParentFile, endOffset)
          }
          true
      }
      val deletable: Iterable[(TopicPartition, Log)] = cleanerManager.deletableLogs()
      deletable.foreach{
        case (topicPartition, log) =>
          try {
            log.deleteOldSegments()
          } finally {
            cleanerManager.doneDeleting(topicPartition)
          }
      }
      if (!cleaned)
        pause(config.backOffMs, TimeUnit.MILLISECONDS)
    }

    /**
     * 注销单个清洁器运行的统计信息。
     */
    def recordStats(id: Int, name: String, from: Long, to: Long, stats: CleanerStats) {
      this.lastStats = stats
      def mb(bytes: Double) = bytes / (1024*1024)
      val message =
        "%n\tLog cleaner thread %d cleaned log %s (dirty section = [%d, %d])%n".format(id, name, from, to) +
        "\t%,.1f MB of log processed in %,.1f seconds (%,.1f MB/sec).%n".format(mb(stats.bytesRead),
                                                                                stats.elapsedSecs,
                                                                                mb(stats.bytesRead/stats.elapsedSecs)) +
        "\tIndexed %,.1f MB in %.1f seconds (%,.1f Mb/sec, %.1f%% of total time)%n".format(mb(stats.mapBytesRead),
                                                                                           stats.elapsedIndexSecs,
                                                                                           mb(stats.mapBytesRead)/stats.elapsedIndexSecs,
                                                                                           100 * stats.elapsedIndexSecs/stats.elapsedSecs) +
        "\tBuffer utilization: %.1f%%%n".format(100 * stats.bufferUtilization) +
        "\tCleaned %,.1f MB in %.1f seconds (%,.1f Mb/sec, %.1f%% of total time)%n".format(mb(stats.bytesRead),
                                                                                           stats.elapsedSecs - stats.elapsedIndexSecs,
                                                                                           mb(stats.bytesRead)/(stats.elapsedSecs - stats.elapsedIndexSecs), 100 * (stats.elapsedSecs - stats.elapsedIndexSecs).toDouble/stats.elapsedSecs) +
        "\tStart size: %,.1f MB (%,d messages)%n".format(mb(stats.bytesRead), stats.messagesRead) +
        "\tEnd size: %,.1f MB (%,d messages)%n".format(mb(stats.bytesWritten), stats.messagesWritten) +
        "\t%.1f%% size reduction (%.1f%% fewer messages)%n".format(100.0 * (1.0 - stats.bytesWritten.toDouble/stats.bytesRead),
                                                                   100.0 * (1.0 - stats.messagesWritten.toDouble/stats.messagesRead))
      info(message)
      if (stats.invalidMessagesRead > 0) {
        warn("\tFound %d invalid messages during compaction.".format(stats.invalidMessagesRead))
      }
    }

  }
}

object LogCleaner {
  val ReconfigurableConfigs = Set(
    KafkaConfig.LogCleanerThreadsProp,
    KafkaConfig.LogCleanerDedupeBufferSizeProp,
    KafkaConfig.LogCleanerDedupeBufferLoadFactorProp,
    KafkaConfig.LogCleanerIoBufferSizeProp,
    KafkaConfig.MessageMaxBytesProp,
    KafkaConfig.LogCleanerIoMaxBytesPerSecondProp,
    KafkaConfig.LogCleanerBackoffMsProp
  )

  def cleanerConfig(config: KafkaConfig): CleanerConfig = {
    CleanerConfig(numThreads = config.logCleanerThreads,
      dedupeBufferSize = config.logCleanerDedupeBufferSize,
      dedupeBufferLoadFactor = config.logCleanerDedupeBufferLoadFactor,
      ioBufferSize = config.logCleanerIoBufferSize,
      maxMessageSize = config.messageMaxBytes,
      maxIoBytesPerSecond = config.logCleanerIoMaxBytesPerSecond,
      backOffMs = config.logCleanerBackoffMs,
      enableCleaner = config.logCleanerEnable)

  }
}

/**
 * This class holds the actual logic for cleaning a log
 * @param id An identifier used for logging
 * @param offsetMap The map used for deduplication
 * @param ioBufferSize The size of the buffers to use. Memory usage will be 2x this number as there is a read and write buffer.
 * @param maxIoBufferSize The maximum size of a message that can appear in the log
 * @param dupBufferLoadFactor The maximum percent full for the deduplication buffer
 * @param throttler The throttler instance to use for limiting I/O rate.
 * @param time The time instance
 * @param checkDone Check if the cleaning for a partition is finished or aborted.
 */
private[log] class Cleaner(val id: Int,
                           val offsetMap: OffsetMap,
                           ioBufferSize: Int,
                           maxIoBufferSize: Int,
                           dupBufferLoadFactor: Double,
                           throttler: Throttler,
                           time: Time,
                           checkDone: (TopicPartition) => Unit) extends Logging {

  protected override def loggerName = classOf[LogCleaner].getName

  this.logIdent = "Cleaner " + id + ": "

  /* buffer used for read i/o */
  private var readBuffer = ByteBuffer.allocate(ioBufferSize)

  /* buffer used for write i/o */
  private var writeBuffer = ByteBuffer.allocate(ioBufferSize)

  private val decompressionBufferSupplier = BufferSupplier.create();

  require(offsetMap.slots * dupBufferLoadFactor > 1, "offset map is too small to fit in even a single message, so log cleaning will never make progress. You can increase log.cleaner.dedupe.buffer.size or decrease log.cleaner.threads")

  /**
    * 清除给定的日志
   *
   * @param cleanable The log to be cleaned
   *
   * @return The first offset not cleaned and the statistics for this round of cleaning
   */
  private[log] def clean(cleanable: LogToClean): (Long, CleanerStats) = {
    // figure out the timestamp below which it is safe to remove delete tombstones
    // this position is defined to be a configurable time beneath the last modified time of the last clean segment
    // 计算出删除删除墓碑的时间戳，该位置被定义为在最后一个干净片段的最后修改时间之下的可配置时间。
    val deleteHorizonMs =
      cleanable.log.logSegments(0, cleanable.firstDirtyOffset).lastOption match {
        case None => 0L
        case Some(seg) => seg.lastModified - cleanable.log.config.deleteRetentionMs
    }

    doClean(cleanable, deleteHorizonMs)
  }

  // 进行实际清除处理
  private[log] def doClean(cleanable: LogToClean, deleteHorizonMs: Long): (Long, CleanerStats) = {
    info("Beginning cleaning of log %s.".format(cleanable.log.name))

    val log = cleanable.log
    val stats = new CleanerStats()

    // build the offset map
    info("Building offset map for %s...".format(cleanable.log.name))
    val upperBoundOffset = cleanable.firstUncleanableOffset
    buildOffsetMap(log, cleanable.firstDirtyOffset, upperBoundOffset, offsetMap, stats) // 根据记录类型构建offsetMap
    val endOffset = offsetMap.latestOffset + 1
    stats.indexDone()

    //确定日志将被清理到的时间戳
    //这是最后一个活动段的下限和压实滞后。
    val cleanableHorizonMs = log.logSegments(0, cleanable.firstUncleanableOffset).lastOption.map(_.lastModified).getOrElse(0L)


    // 将段分组然后清洁组
    info("Cleaning log %s (cleaning prior to %s, discarding tombstones prior to %s)...".format(log.name, new Date(cleanableHorizonMs), new Date(deleteHorizonMs)))
    for (group <- groupSegmentsBySize(log.logSegments(0, endOffset), log.config.segmentSize, log.config.maxIndexSize, cleanable.firstUncleanableOffset))
      cleanSegments(log, group, offsetMap, deleteHorizonMs, stats)

    // record buffer utilization
    stats.bufferUtilization = offsetMap.utilization

    stats.allDone()// 标记结束

    (endOffset, stats)
  }

  /**
   * Clean a group of segments into a single replacement segment
   *
   * @param log The log being cleaned
   * @param segments The group of segments being cleaned
   * @param map The offset map to use for cleaning segments
   * @param deleteHorizonMs 保留删除墓碑的时间
   * @param stats Collector for cleaning statistics
   */
  private[log] def cleanSegments(log: Log,
                                 segments: Seq[LogSegment],
                                 map: OffsetMap,
                                 deleteHorizonMs: Long,
                                 stats: CleanerStats) {

    def deleteCleanedFileIfExists(file: File): Unit = {
      Files.deleteIfExists(new File(file.getPath + Log.CleanedFileSuffix).toPath)
    }

    // create a new segment with a suffix appended to the name of the log and indexes
    val firstSegment = segments.head
    deleteCleanedFileIfExists(firstSegment.log.file)
    deleteCleanedFileIfExists(firstSegment.offsetIndex.file)
    deleteCleanedFileIfExists(firstSegment.timeIndex.file)
    deleteCleanedFileIfExists(firstSegment.txnIndex.file)

    // 最终合成的那个段
    val baseOffset = firstSegment.baseOffset
    val cleaned = LogSegment.open(log.dir, baseOffset, log.config, time, fileSuffix = Log.CleanedFileSuffix,
      initFileSize = log.initFileSize, preallocate = log.config.preallocate)

    try {
      // 将新片段细分为新的目标段
      val iter = segments.iterator
      var currentSegmentOpt: Option[LogSegment] = Some(iter.next())
      while (currentSegmentOpt.isDefined) {
        val currentSegment = currentSegmentOpt.get// 当前处理的段
        val nextSegmentOpt = if (iter.hasNext) Some(iter.next()) else None

        val startOffset = currentSegment.baseOffset  // 当前回收段的起始offset
        val upperBoundOffset = nextSegmentOpt.map(_.baseOffset).getOrElse(map.latestOffset + 1) // 下一个段的起始offset作为结束offset
        val abortedTransactions = log.collectAbortedTransactions(startOffset, upperBoundOffset)// 收集区间内偏移量
        val transactionMetadata = CleanedTransactionMetadata(abortedTransactions, Some(cleaned.txnIndex))

        val retainDeletes = currentSegment.lastModified > deleteHorizonMs
        info(s"Cleaning segment $startOffset in log ${log.name} (largest timestamp ${new Date(currentSegment.largestTimestamp)}) " +
          s"into ${cleaned.baseOffset}, ${if(retainDeletes) "retaining" else "discarding"} deletes.")
        cleanInto(log.topicPartition, currentSegment.log, cleaned, map, retainDeletes, log.config.maxMessageSize,
          transactionMetadata, log.activeProducersWithLastSequence, stats)

        currentSegmentOpt = nextSegmentOpt
      }

      cleaned.onBecomeInactiveSegment()
      // flush new segment to disk before swap
      cleaned.flush()

      // update the modification date to retain the last modified date of the original files
      val modified = segments.last.lastModified
      cleaned.lastModified = modified

      // swap in new segment
      info(s"Swapping in cleaned segment ${cleaned.baseOffset} for segment(s) ${segments.map(_.baseOffset).mkString(",")} " +
        s"in log ${log.name}")
      log.replaceSegments(cleaned, segments)
    } catch {
      case e: LogCleaningAbortedException =>
        try cleaned.deleteIfExists()
        catch {
          case deleteException: Exception =>
            e.addSuppressed(deleteException)
        } finally throw e
    }
  }

  /**
   * 使用提供的key=>offset映射将给定的源日志段清理到目标段中
   * @param topicPartition The topic and partition of the log segment to clean
   * @param sourceRecords The dirty log segment
   * @param dest The cleaned log segment
   * @param map The key=>offset mapping
   * @param retainDeletes Should delete tombstones be retained while cleaning this segment
   * @param maxLogMessageSize The maximum message size of the corresponding topic
   * @param stats Collector for cleaning statistics
   */
  private[log] def cleanInto(topicPartition: TopicPartition,
                             sourceRecords: FileRecords,
                             dest: LogSegment,
                             map: OffsetMap,
                             retainDeletes: Boolean,
                             maxLogMessageSize: Int,
                             transactionMetadata: CleanedTransactionMetadata,
                             activeProducers: Map[Long, Int],
                             stats: CleanerStats) {
    val logCleanerFilter = new RecordFilter {
      var discardBatchRecords: Boolean = _

      override def checkBatchRetention(batch: RecordBatch): BatchRetention = {
        // we piggy-back on the tombstone retention logic to delay deletion of transaction markers.
        // note that we will never delete a marker until all the records from that transaction are removed.
        discardBatchRecords = shouldDiscardBatch(batch, transactionMetadata, retainTxnMarkers = retainDeletes)

        // 检查批次是否包含生产者的最后序列号。 如果是这样，我们不能删除该批次，否则生产者可能会看到一个无序的错误。
        if (batch.hasProducerId && activeProducers.get(batch.producerId).contains(batch.lastSequence))
          BatchRetention.RETAIN_EMPTY
        else if (discardBatchRecords)
          BatchRetention.DELETE
        else
          BatchRetention.DELETE_EMPTY
      }

      override def shouldRetainRecord(batch: RecordBatch, record: Record): Boolean = {
        if (discardBatchRecords)
          // The batch is only retained to preserve producer sequence information; the records can be removed
          false
        else
          Cleaner.this.shouldRetainRecord(map, retainDeletes, batch, record, stats)
      }
    }

    var position = 0
    while (position < sourceRecords.sizeInBytes) {
      checkDone(topicPartition)
      // read a chunk of messages and copy any that are to be retained to the write buffer to be written out
      readBuffer.clear()
      writeBuffer.clear()

      sourceRecords.readInto(readBuffer, position)
      val records = MemoryRecords.readableRecords(readBuffer)// 从文件中读取MemoryRecord
      throttler.maybeThrottle(records.sizeInBytes)
      // 过滤记录
      val result = records.filterTo(topicPartition, logCleanerFilter, writeBuffer, maxLogMessageSize, decompressionBufferSupplier)
      stats.readMessages(result.messagesRead, result.bytesRead)
      stats.recopyMessages(result.messagesRetained, result.bytesRetained)

      position += result.bytesRead

      // if any messages are to be retained, write them out
      val outputBuffer = result.output
      if (outputBuffer.position() > 0) {
        outputBuffer.flip()
        val retained = MemoryRecords.readableRecords(outputBuffer)
        // it's OK not to hold the Log's lock in this case, because this segment is only accessed by other threads
        // after `Log.replaceSegments` (which acquires the lock) is called
        dest.append(firstOffset = retained.batches.iterator.next().baseOffset,
          largestOffset = result.maxOffset,
          largestTimestamp = result.maxTimestamp,
          shallowOffsetOfMaxTimestamp = result.shallowOffsetOfMaxTimestamp,
          records = retained)
        throttler.maybeThrottle(outputBuffer.limit())
      }

      // if we read bytes but didn't get even one complete message, our I/O buffer is too small, grow it and try again
      if (readBuffer.limit() > 0 && result.messagesRead == 0)
        growBuffers(maxLogMessageSize)
    }
    restoreBuffers()
  }

  // 是否应该丢弃批次
  private def shouldDiscardBatch(batch: RecordBatch,
                                 transactionMetadata: CleanedTransactionMetadata,
                                 retainTxnMarkers: Boolean): Boolean = {
    if (batch.isControlBatch) {
      val canDiscardControlBatch = transactionMetadata.onControlBatchRead(batch)
      canDiscardControlBatch && !retainTxnMarkers
    } else {
      val canDiscardBatch = transactionMetadata.onBatchRead(batch)
      canDiscardBatch
    }
  }

  private def shouldRetainRecord(map: kafka.log.OffsetMap,
                                 retainDeletes: Boolean,
                                 batch: RecordBatch,
                                 record: Record,
                                 stats: CleanerStats): Boolean = {
    val pastLatestOffset = record.offset > map.latestOffset
    if (pastLatestOffset)
      return true

    if (record.hasKey) {
      val key = record.key
      val foundOffset = map.get(key)
      /* two cases in which we can get rid of a message:
       *   1) if there exists a message with the same key but higher offset
       *   2) if the message is a delete "tombstone" marker and enough time has passed
       */
      val redundant = foundOffset >= 0 && record.offset < foundOffset
      val obsoleteDelete = !retainDeletes && !record.hasValue
      !redundant && !obsoleteDelete
    } else {
      stats.invalidMessage()
      false
    }
  }

  /**
   * 在限定情况下，升级双倍I/O缓存
   */
  def growBuffers(maxLogMessageSize: Int) {
    val maxBufferSize = math.max(maxLogMessageSize, maxIoBufferSize)
    if(readBuffer.capacity >= maxBufferSize || writeBuffer.capacity >= maxBufferSize)
      throw new IllegalStateException("This log contains a message larger than maximum allowable size of %s.".format(maxBufferSize))
    val newSize = math.min(this.readBuffer.capacity * 2, maxBufferSize)
    info("Growing cleaner I/O buffers from " + readBuffer.capacity + "bytes to " + newSize + " bytes.")
    this.readBuffer = ByteBuffer.allocate(newSize)
    this.writeBuffer = ByteBuffer.allocate(newSize)
  }

  /**
   * 将I/O缓冲容量恢复到原来的大小
   */
  def restoreBuffers() {
    if(this.readBuffer.capacity > this.ioBufferSize)
      this.readBuffer = ByteBuffer.allocate(this.ioBufferSize)
    if(this.writeBuffer.capacity > this.ioBufferSize)
      this.writeBuffer = ByteBuffer.allocate(this.ioBufferSize)
  }

  /**
    * 将日志中的段分组为小于给定大小的组。对日志数据和索引数据分别执行大小。
    *我们收集一组这样的段一起成为一个单一的目的段。这防止了段大小收缩太多。
   *
   * @param segments The log segments to group
   * @param maxSize the maximum size in bytes for the total of all log data in a group
   * @param maxIndexSize the maximum size in bytes for the total of all index data in a group
   *
   * @return A list of grouped segments
   */
  private[log] def groupSegmentsBySize(segments: Iterable[LogSegment], maxSize: Int, maxIndexSize: Int, firstUncleanableOffset: Long): List[Seq[LogSegment]] = {
    var grouped = List[List[LogSegment]]()
    var segs = segments.toList
    while(segs.nonEmpty) {
      var group = List(segs.head)
      var logSize = segs.head.size.toLong
      var indexSize = segs.head.offsetIndex.sizeInBytes.toLong
      var timeIndexSize = segs.head.timeIndex.sizeInBytes.toLong
      segs = segs.tail
      while(segs.nonEmpty &&
            logSize + segs.head.size <= maxSize &&
            indexSize + segs.head.offsetIndex.sizeInBytes <= maxIndexSize &&
            timeIndexSize + segs.head.timeIndex.sizeInBytes <= maxIndexSize &&
            lastOffsetForFirstSegment(segs, firstUncleanableOffset) - group.last.baseOffset <= Int.MaxValue) {
        group = segs.head :: group
        logSize += segs.head.size
        indexSize += segs.head.offsetIndex.sizeInBytes
        timeIndexSize += segs.head.timeIndex.sizeInBytes
        segs = segs.tail
      }
      grouped ::= group.reverse
    }
    grouped.reverse
  }

  /**
    * We want to get the last offset in the first log segment in segs.
    * LogSegment.nextOffset() gives the exact last offset in a segment, but can be expensive since it requires
    * scanning the segment from the last index entry.
    * Therefore, we estimate the last offset of the first log segment by using
    * the base offset of the next segment in the list.
    * If the next segment doesn't exist, first Uncleanable Offset will be used.
    *
    * @param segs - remaining segments to group.
    * @return The estimated last offset for the first segment in segs
    */
  private def lastOffsetForFirstSegment(segs: List[LogSegment], firstUncleanableOffset: Long): Long = {
    if (segs.size > 1) {
      /* if there is a next segment, use its base offset as the bounding offset to guarantee we know
       * the worst case offset */
      segs(1).baseOffset - 1
    } else {
      //for the last segment in the list, use the first uncleanable offset.
      firstUncleanableOffset - 1
    }
  }

  /**
    * 为清洁中使用的日志的可清洗脏部分中的键构建key_hash => offset的映射。
   * @param log The log to use
   * @param start The offset at which dirty messages begin
   * @param end The ending offset for the map that is being built
   * @param map The map in which to store the mappings
   * @param stats Collector for cleaning statistics
   */
  private[log] def buildOffsetMap(log: Log,
                                  start: Long,
                                  end: Long,
                                  map: OffsetMap,
                                  stats: CleanerStats) {
    map.clear()
    val dirty = log.logSegments(start, end).toBuffer // 需要回收的脏数据段
    info("Building offset map for log %s for %d segments in offset range [%d, %d).".format(log.name, dirty.size, start, end))

    val abortedTransactions = log.collectAbortedTransactions(start, end)
    val transactionMetadata = CleanedTransactionMetadata(abortedTransactions)

    // Add all the cleanable dirty segments. We must take at least map.slots * load_factor,
    // but we may be able to fit more (if there is lots of duplication in the dirty section of the log)
    var full = false
    for (segment <- dirty if !full) {
      checkDone(log.topicPartition)// 检查清除状态

      full = buildOffsetMapForSegment(log.topicPartition, segment, map, start, log.config.maxMessageSize,
        transactionMetadata, stats)
      if (full)
        debug("Offset map is full, %d segments fully mapped, segment with base offset %d is partially mapped".format(dirty.indexOf(segment), segment.baseOffset))
    }
    info("Offset map for log %s complete.".format(log.name))
  }

  /**
   * Add the messages in the given segment to the offset map
   * 添加消息到给定的segment
   * @param segment The segment to index
   * @param map The map in which to store the key=>offset mapping
   * @param stats Collector for cleaning statistics
   *
   * @return If the map was filled whilst loading from this segment
   */
  private def buildOffsetMapForSegment(topicPartition: TopicPartition,
                                       segment: LogSegment,
                                       map: OffsetMap,
                                       startOffset: Long,
                                       maxLogMessageSize: Int,
                                       transactionMetadata: CleanedTransactionMetadata,
                                       stats: CleanerStats): Boolean = {
    var position = segment.offsetIndex.lookup(startOffset).position // 目标offset索引文件的起始偏移量
    val maxDesiredMapSize = (map.slots * this.dupBufferLoadFactor).toInt
    while (position < segment.log.sizeInBytes) {
      checkDone(topicPartition)
      readBuffer.clear()
      segment.log.readInto(readBuffer, position)
      val records = MemoryRecords.readableRecords(readBuffer)// 读取的记录
      throttler.maybeThrottle(records.sizeInBytes)// 控制速率

      val startPosition = position
      for (batch <- records.batches.asScala) {
        if (batch.isControlBatch) {
          transactionMetadata.onControlBatchRead(batch)
          stats.indexMessagesRead(1)
        } else {
          val isAborted = transactionMetadata.onBatchRead(batch)
          if (isAborted) {
            // 如果批处理被中止，请不要打扰填充offset map。
            // 注意，在V2和以上支持中止标记，这意味着计数被定义。
            stats.indexMessagesRead(batch.countOrNull)
          } else {
            for (record <- batch.asScala) {
              if (record.hasKey && record.offset >= startOffset) {
                if (map.size < maxDesiredMapSize)// 向偏移offsetmap中添加记录
                  map.put(record.key, record.offset)
                else
                  return true
              }
              stats.indexMessagesRead(1)
            }
          }
        }

        if (batch.lastOffset >= startOffset)
          map.updateLatestOffset(batch.lastOffset)// 更新offset中的最新偏移量
      }
      val bytesRead = records.validBytes // 将已处理的记录进行更新偏移量和stats
      position += bytesRead
      stats.indexBytesRead(bytesRead)

      // 如果我们没有读取一个完整的消息，那么我们的读取缓冲区可能太小了。
      if(position == startPosition)
        growBuffers(maxLogMessageSize)
    }
    restoreBuffers()
    false
  }
}

/**
 * A simple struct for collecting stats about log cleaning
 */
private class CleanerStats(time: Time = Time.SYSTEM) {
  val startTime = time.milliseconds
  var mapCompleteTime = -1L
  var endTime = -1L
  var bytesRead = 0L
  var bytesWritten = 0L
  var mapBytesRead = 0L
  var mapMessagesRead = 0L
  var messagesRead = 0L
  var invalidMessagesRead = 0L
  var messagesWritten = 0L
  var bufferUtilization = 0.0d

  def readMessages(messagesRead: Int, bytesRead: Int) {
    this.messagesRead += messagesRead
    this.bytesRead += bytesRead
  }

  def invalidMessage() {
    invalidMessagesRead += 1
  }

  def recopyMessages(messagesWritten: Int, bytesWritten: Int) {
    this.messagesWritten += messagesWritten
    this.bytesWritten += bytesWritten
  }

  def indexMessagesRead(size: Int) {
    mapMessagesRead += size
  }

  def indexBytesRead(size: Int) {
    mapBytesRead += size
  }

  def indexDone() {
    mapCompleteTime = time.milliseconds
  }

  def allDone() {
    endTime = time.milliseconds
  }

  def elapsedSecs = (endTime - startTime)/1000.0

  def elapsedIndexSecs = (mapCompleteTime - startTime)/1000.0

}

/**
 * 日志的Help类，其主题/分区，第一个可清除位置，以及第一个不可清洗的脏位置
 */
private case class LogToClean(topicPartition: TopicPartition, log: Log, firstDirtyOffset: Long, uncleanableOffset: Long) extends Ordered[LogToClean] {
  val cleanBytes = log.logSegments(-1, firstDirtyOffset).map(_.size.toLong).sum
  private[this] val firstUncleanableSegment = log.logSegments(uncleanableOffset, log.activeSegment.baseOffset).headOption.getOrElse(log.activeSegment)
  val firstUncleanableOffset = firstUncleanableSegment.baseOffset
  val cleanableBytes = log.logSegments(firstDirtyOffset, math.max(firstDirtyOffset, firstUncleanableOffset)).map(_.size.toLong).sum
  val totalBytes = cleanBytes + cleanableBytes
  val cleanableRatio = cleanableBytes / totalBytes.toDouble
  override def compare(that: LogToClean): Int = math.signum(this.cleanableRatio - that.cleanableRatio).toInt
}

private[log] object CleanedTransactionMetadata {
  def apply(abortedTransactions: List[AbortedTxn],
            transactionIndex: Option[TransactionIndex] = None): CleanedTransactionMetadata = {
    val queue = mutable.PriorityQueue.empty[AbortedTxn](new Ordering[AbortedTxn] {// 优先级队列
      override def compare(x: AbortedTxn, y: AbortedTxn): Int = x.firstOffset compare y.firstOffset
    }.reverse)
    queue ++= abortedTransactions
    new CleanedTransactionMetadata(queue, transactionIndex)
  }

  val Empty = CleanedTransactionMetadata(List.empty[AbortedTxn])
}

/**
 * This is a helper class to facilitate tracking transaction state while cleaning the log. It is initialized
 * with the aborted transactions from the transaction index and its state is updated as the cleaner iterates through
 * the log during a round of cleaning. This class is responsible for deciding when transaction markers can
 * be removed and is therefore also responsible for updating the cleaned transaction index accordingly.
 */
private[log] class CleanedTransactionMetadata(val abortedTransactions: mutable.PriorityQueue[AbortedTxn],
                                              val transactionIndex: Option[TransactionIndex] = None) {
  val ongoingCommittedTxns = mutable.Set.empty[Long]
  val ongoingAbortedTxns = mutable.Map.empty[Long, AbortedTransactionMetadata]

  /**
   * Update the cleaned transaction state with a control batch that has just been traversed by the cleaner.
   * Return true if the control batch can be discarded.
   */
  def onControlBatchRead(controlBatch: RecordBatch): Boolean = {
    consumeAbortedTxnsUpTo(controlBatch.lastOffset)

    // 获取控制批次类型
    val controlRecord = controlBatch.iterator.next()
    val controlType = ControlRecordType.parse(controlRecord.key)
    val producerId = controlBatch.producerId
    controlType match {
      case ControlRecordType.ABORT =>
        ongoingAbortedTxns.remove(producerId) match {
          // Retain the marker until all batches from the transaction have been removed
          case Some(abortedTxnMetadata) if abortedTxnMetadata.lastObservedBatchOffset.isDefined =>
            transactionIndex.foreach(_.append(abortedTxnMetadata.abortedTxn))
            false
          case _ => true
        }

      case ControlRecordType.COMMIT =>
        // 如果我们没有遍历交易中的任何批次，则此标记符合删除条件
        !ongoingCommittedTxns.remove(producerId)

      case _ => false
    }
  }

  // 将小于offset的abortedTxn添加到ongoingAbortedTxns
  private def consumeAbortedTxnsUpTo(offset: Long): Unit = {
    while (abortedTransactions.headOption.exists(_.firstOffset <= offset)) {
      val abortedTxn = abortedTransactions.dequeue()
      ongoingAbortedTxns += abortedTxn.producerId -> new AbortedTransactionMetadata(abortedTxn)
    }
  }

  /**
   * Update the transactional state for the incoming non-control batch. If the batch is part of
   * an aborted transaction, return true to indicate that it is safe to discard.
    * 更新传入非控制批处理的事务状态。如果批处理是中止事务的一部分，则返回TRUE以指示放弃该操作是安全的。
   */
  def onBatchRead(batch: RecordBatch): Boolean = {
    consumeAbortedTxnsUpTo(batch.lastOffset)
    if (batch.isTransactional) {
      ongoingAbortedTxns.get(batch.producerId) match {
        case Some(abortedTransactionMetadata) =>
          abortedTransactionMetadata.lastObservedBatchOffset = Some(batch.lastOffset)
          true
        case None =>
          ongoingCommittedTxns += batch.producerId
          false
      }
    } else {
      false
    }
  }

}

private class AbortedTransactionMetadata(val abortedTxn: AbortedTxn) {
  // 观察到的批次最新的offset
  var lastObservedBatchOffset: Option[Long] = None
}
