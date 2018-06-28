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

import java.io.File
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

import com.yammer.metrics.core.Gauge
import kafka.common.LogCleaningAbortedException
import kafka.metrics.KafkaMetricsGroup
import kafka.server.LogDirFailureChannel
import kafka.server.checkpoints.OffsetCheckpointFile
import kafka.utils.CoreUtils._
import kafka.utils.{Logging, Pool}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.errors.KafkaStorageException

import scala.collection.{immutable, mutable}

// 日志清除的状态
private[log] sealed trait LogCleaningState
private[log] case object LogCleaningInProgress extends LogCleaningState
private[log] case object LogCleaningAborted extends LogCleaningState
private[log] case object LogCleaningPaused extends LogCleaningState

/**
  * 只是清除状态管理
 *  Manage the state of each partition being cleaned.
 *  If a partition is to be cleaned, it enters the LogCleaningInProgress state.
 *  While a partition is being cleaned, it can be requested to be aborted and paused. Then the partition first enters
 *  the LogCleaningAborted state. Once the cleaning task is aborted, the partition enters the LogCleaningPaused state.
 *  While a partition is in the LogCleaningPaused state, it won't be scheduled for cleaning again, until cleaning is
 *  requested to be resumed.
 */
private[log] class LogCleanerManager(val logDirs: Seq[File],
                                     val logs: Pool[TopicPartition, Log],
                                     val logDirFailureChannel: LogDirFailureChannel) extends Logging with KafkaMetricsGroup {

  import LogCleanerManager._

  protected override def loggerName = classOf[LogCleaner].getName

  // package-private for testing
  private[log] val offsetCheckpointFile = "cleaner-offset-checkpoint"

  /* 每个日志保存最后清除点的偏移检查点 */
  @volatile private var checkpoints = logDirs.map(dir =>
    (dir, new OffsetCheckpointFile(new File(dir, offsetCheckpointFile), logDirFailureChannel))).toMap

  /* 当前正在清理的日志集 */
  private val inProgress = mutable.HashMap[TopicPartition, LogCleaningState]()

  /* 全局锁，用于控制进程中的所有访问和偏移检查点。 */
  private val lock = new ReentrantLock

  /* 用于协调隔板的停工和清洗 */
  private val pausedCleaningCond = lock.newCondition()

  /* a gauge for tracking the cleanable ratio of the dirtiest log */
  @volatile private var dirtiestLogCleanableRatio = 0.0
  newGauge("max-dirty-percent", new Gauge[Int] { def value = (100 * dirtiestLogCleanableRatio).toInt })

  /* a gauge for tracking the time since the last log cleaner run, in milli seconds */
  @volatile private var timeOfLastRun : Long = Time.SYSTEM.milliseconds
  newGauge("time-since-last-run-ms", new Gauge[Long] { def value = Time.SYSTEM.milliseconds - timeOfLastRun })

  /**
   * @return the position processed for all logs.
   */
  def allCleanerCheckpoints: Map[TopicPartition, Long] = {
    inLock(lock) {
      checkpoints.values.flatMap(checkpoint => {
        try {
          checkpoint.read()
        } catch {
          case e: KafkaStorageException =>
            error(s"Failed to access checkpoint file ${checkpoint.file.getName} in dir ${checkpoint.file.getParentFile.getAbsolutePath}", e)
            Map.empty[TopicPartition, Long]
        }
      }).toMap
    }
  }

  /**
    * Package private for unit test. Get the cleaning state of the partition.
    */
  private[log] def cleaningState(tp: TopicPartition): Option[LogCleaningState] = {
    inLock(lock) {
      inProgress.get(tp)
    }
  }

  /**
    * Package private for unit test. Set the cleaning state of the partition.
    */
  private[log] def setCleaningState(tp: TopicPartition, state: LogCleaningState): Unit = {
    inLock(lock) {
      inProgress.put(tp, state)
    }
  }

   /**
     * 选择下一步清理日志并将其添加到正在进行中的集合中。
     * 我们每次从完整的日志集重新计算这一点，以允许日志动态添加到日志管理器维护的日志池中。
    */
  def grabFilthiestCompactedLog(time: Time): Option[LogToClean] = {
    inLock(lock) {// 防止多线程获取
      val now = time.milliseconds
      this.timeOfLastRun = now
      val lastClean = allCleanerCheckpoints
      val dirtyLogs = logs.filter {
        case (_, log) => log.config.compact  // 标记为压缩的匹配日志
      }.filterNot {
        case (topicPartition, _) => inProgress.contains(topicPartition) // 跳过已在进行中的任何日志
      }.map {
        case (topicPartition, log) => // 为每个创建一个LogtoCurror实例
          val (firstDirtyOffset, firstUncleanableDirtyOffset) = LogCleanerManager.cleanableOffsets(log, topicPartition,
            lastClean, now)
          LogToClean(topicPartition, log, firstDirtyOffset, firstUncleanableDirtyOffset)
      }.filter(ltc => ltc.totalBytes > 0) // 跳过任何空日志

      this.dirtiestLogCleanableRatio = if (dirtyLogs.nonEmpty) dirtyLogs.max.cleanableRatio else 0
      // and must meet the minimum threshold for dirty byte ratio
      val cleanableLogs = dirtyLogs.filter(ltc => ltc.cleanableRatio > ltc.log.config.minCleanableRatio)
      if(cleanableLogs.isEmpty) {
        None
      } else {
        val filthiest = cleanableLogs.max
        inProgress.put(filthiest.topicPartition, LogCleaningInProgress)
        Some(filthiest)
      }
    }
  }

  /**
    * Find any logs that have compact and delete enabled
    */
  def deletableLogs(): Iterable[(TopicPartition, Log)] = {
    inLock(lock) {
      val toClean = logs.filter { case (topicPartition, log) =>
        !inProgress.contains(topicPartition) && isCompactAndDelete(log)
      }
      toClean.foreach { case (tp, _) => inProgress.put(tp, LogCleaningInProgress) }
      toClean
    }
  }

  /**
   *  Abort the cleaning of a particular partition, if it's in progress. This call blocks until the cleaning of
   *  the partition is aborted.
   *  This is implemented by first abortAndPausing and then resuming the cleaning of the partition.
    * 跳出清除
   */
  def abortCleaning(topicPartition: TopicPartition) {
    inLock(lock) {
      abortAndPauseCleaning(topicPartition)
      resumeCleaning(topicPartition)
    }
    info(s"The cleaning for partition $topicPartition is aborted")
  }

  /**
    * 如果在进行中，则中断清除特定的分区，并暂停任何未来清洗这个分区。此调用阻塞直到分区的清理中止并暂停。
   *  1. 如果分区不在进行中，则将其标记为暂停。
   *  2. 否则，首先将分区的状态标记为中止。
   *  3. 清洁的线程定期检查状态，如果它看到分区的状态被中止，则抛出LogCleaningAbortedException来停止清理任务。
   *  4. 当清洁任务停止时，将调用doneCleaning（），将分区的状态设置为暂停状态。
   *  5. abortAndPauseCleaning（）等待分区的状态改变为暂停状态。
   */
  def abortAndPauseCleaning(topicPartition: TopicPartition) {
    inLock(lock) {
      inProgress.get(topicPartition) match {
        case None =>
          inProgress.put(topicPartition, LogCleaningPaused)
        case Some(state) =>
          state match {
            case LogCleaningInProgress =>
              inProgress.put(topicPartition, LogCleaningAborted)// 中断状态
            case LogCleaningPaused =>
            case s =>
              throw new IllegalStateException(s"Compaction for partition $topicPartition cannot be aborted and paused since it is in $s state.")
          }
      }
      while (!isCleaningInState(topicPartition, LogCleaningPaused))// 持续监测topicPartition的删除状态（暂停）
        pausedCleaningCond.await(100, TimeUnit.MILLISECONDS)
    }
    info(s"The cleaning for partition $topicPartition is aborted and paused")
  }

  /**
   *  清理已暂停的分区。
   */
  def resumeCleaning(topicPartition: TopicPartition) {
    inLock(lock) {
      inProgress.get(topicPartition) match {
        case None =>
          throw new IllegalStateException(s"Compaction for partition $topicPartition cannot be resumed since it is not paused.")
        case Some(state) =>
          state match {
            case LogCleaningPaused =>
              inProgress.remove(topicPartition)
            case s =>
              throw new IllegalStateException(s"Compaction for partition $topicPartition cannot be resumed since it is in $s state.")
          }
      }
    }
    info(s"Compaction for partition $topicPartition is resumed")
  }

  /**
   *  检查分区的清理是否处于特定状态。 呼叫者预计在拨打电话时保持锁定状态。
   */
  private def isCleaningInState(topicPartition: TopicPartition, expectedState: LogCleaningState): Boolean = {
    inProgress.get(topicPartition) match {
      case None => false
      case Some(state) =>
        if (state == expectedState)
          true
        else
          false
    }
  }

  /**
   *  Check if the cleaning for a partition is aborted. If so, throw an exception.
   */
  def checkCleaningAborted(topicPartition: TopicPartition) {
    inLock(lock) {
      if (isCleaningInState(topicPartition, LogCleaningAborted))
        throw new LogCleaningAbortedException()
    }
  }

  def updateCheckpoints(dataDir: File, update: Option[(TopicPartition,Long)]) {
    inLock(lock) {
      val checkpoint = checkpoints(dataDir)
      if (checkpoint != null) {
        try {
          val existing = checkpoint.read().filterKeys(logs.keys) ++ update
          checkpoint.write(existing)
        } catch {
          case e: KafkaStorageException =>
            error(s"Failed to access checkpoint file ${checkpoint.file.getName} in dir ${checkpoint.file.getParentFile.getAbsolutePath}", e)
        }
      }
    }
  }

  def alterCheckpointDir(topicPartition: TopicPartition, sourceLogDir: File, destLogDir: File): Unit = {
    inLock(lock) {
      try {
        checkpoints.get(sourceLogDir).flatMap(_.read().get(topicPartition)) match {
          case Some(offset) =>
            // Remove this partition from the checkpoint file in the source log directory
            updateCheckpoints(sourceLogDir, None)
            // Add offset for this partition to the checkpoint file in the source log directory
            updateCheckpoints(destLogDir, Option(topicPartition, offset))
          case None =>
        }
      } catch {
        case e: KafkaStorageException =>
          error(s"Failed to access checkpoint file in dir ${sourceLogDir.getAbsolutePath}", e)
      }
    }
  }

  def handleLogDirFailure(dir: String) {
    info(s"Stopping cleaning logs in dir $dir")
    inLock(lock) {
      checkpoints = checkpoints.filterKeys(_.getAbsolutePath != dir)
    }
  }

  def maybeTruncateCheckpoint(dataDir: File, topicPartition: TopicPartition, offset: Long) {
    inLock(lock) {
      if (logs.get(topicPartition).config.compact) {
        val checkpoint = checkpoints(dataDir)
        if (checkpoint != null) {
          val existing = checkpoint.read() // 以存在的检查点
          if (existing.getOrElse(topicPartition, 0L) > offset)
            checkpoint.write(existing + (topicPartition -> offset)) // 写入新的tp检查点
        }
      }
    }
  }

  /**
   * Save out the endOffset and remove the given log from the in-progress set, if not aborted.
   */
  def doneCleaning(topicPartition: TopicPartition, dataDir: File, endOffset: Long) {
    inLock(lock) {
      inProgress.get(topicPartition) match {
        case Some(LogCleaningInProgress) =>
          updateCheckpoints(dataDir, Option(topicPartition, endOffset))
          inProgress.remove(topicPartition)
        case Some(LogCleaningAborted) =>
          inProgress.put(topicPartition, LogCleaningPaused)
          pausedCleaningCond.signalAll()
        case None =>
          throw new IllegalStateException(s"State for partition $topicPartition should exist.")
        case s =>
          throw new IllegalStateException(s"In-progress partition $topicPartition cannot be in $s state.")
      }
    }
  }

  def doneDeleting(topicPartition: TopicPartition): Unit = {
    inLock(lock) {
      inProgress.get(topicPartition) match {
        case Some(LogCleaningInProgress) =>
          inProgress.remove(topicPartition)
        case Some(LogCleaningAborted) =>
          inProgress.put(topicPartition, LogCleaningPaused)
          pausedCleaningCond.signalAll()
        case None =>
          throw new IllegalStateException(s"State for partition $topicPartition should exist.")
        case s =>
          throw new IllegalStateException(s"In-progress partition $topicPartition cannot be in $s state.")
      }
    }
  }
}

private[log] object LogCleanerManager extends Logging {

  // 压缩和删除
  def isCompactAndDelete(log: Log): Boolean = {
    log.config.compact && log.config.delete
  }


  /**
    * 返回可清除的脏偏移范围。
    *
    * @param log the log
    * @param lastClean the map of checkpointed offsets
    * @param now the current time in milliseconds of the cleaning operation
    * @return 下（包含）和上（排他性）偏移
    */
  def cleanableOffsets(log: Log, topicPartition: TopicPartition, lastClean: immutable.Map[TopicPartition, Long], now: Long): (Long, Long) = {

    // 检查点偏移，即，下一个脏段的第一个偏移量
    val lastCleanOffset: Option[Long] = lastClean.get(topicPartition)

    // If the log segments are abnormally truncated and hence the checkpointed offset is no longer valid;
    // reset to the log starting offset and log the error
    val logStartOffset = log.logSegments.head.baseOffset
    val firstDirtyOffset = { // 刚开始移除的偏移量
      val offset = lastCleanOffset.getOrElse(logStartOffset)
      if (offset < logStartOffset) {
        // don't bother with the warning if compact and delete are enabled.
        if (!isCompactAndDelete(log))
          warn(s"Resetting first dirty offset of ${log.name} to log start offset $logStartOffset since the checkpointed offset $offset is invalid.")
        logStartOffset
      } else {
        offset
      }
    }

    val compactionLagMs = math.max(log.config.compactionLagMs, 0L)

    // 查找无法清洗的第一段
    // 无论是活动段，还是任何消息段，都比最短的压缩滞后时间更接近日志头。
    val firstUncleanableDirtyOffset: Long = Seq(

      // 我们不清除超出第一个不稳定的偏移
      log.firstUnstableOffset.map(_.messageOffset),

      // 活动段始终是不可清洗的。
      Option(log.activeSegment.baseOffset),

      // 第一段的最大消息时间戳是从现在开始的最小时间间隔内的
      if (compactionLagMs > 0) {
        // 脏日志段
        val dirtyNonActiveSegments = log.logSegments(firstDirtyOffset, log.activeSegment.baseOffset)
        dirtyNonActiveSegments.find { s =>
          val isUncleanable = s.largestTimestamp > now - compactionLagMs
          debug(s"Checking if log segment may be cleaned: log='${log.name}' segment.baseOffset=${s.baseOffset} segment.largestTimestamp=${s.largestTimestamp}; now - compactionLag=${now - compactionLagMs}; is uncleanable=$isUncleanable")
          isUncleanable
        }.map(_.baseOffset)// 清除的日志段
      } else None
    ).flatten.min

    debug(s"Finding range of cleanable offsets for log=${log.name} topicPartition=$topicPartition. Last clean offset=$lastCleanOffset now=$now => firstDirtyOffset=$firstDirtyOffset firstUncleanableOffset=$firstUncleanableDirtyOffset activeSegment.baseOffset=${log.activeSegment.baseOffset}")

    (firstDirtyOffset, firstUncleanableDirtyOffset)
  }
}
