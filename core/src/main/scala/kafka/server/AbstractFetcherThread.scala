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

package kafka.server

import java.util.concurrent.locks.ReentrantLock

import kafka.cluster.BrokerEndPoint
import kafka.utils.{DelayedItem, Pool, ShutdownableThread}
import org.apache.kafka.common.errors.{CorruptRecordException, KafkaStorageException}
import kafka.common.{ClientIdAndBroker, KafkaException}
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.CoreUtils.inLock
import org.apache.kafka.common.protocol.Errors
import AbstractFetcherThread._

import scala.collection.{Map, Set, mutable}
import scala.collection.JavaConverters._
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import com.yammer.metrics.core.Gauge
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.{FatalExitError, PartitionStates}
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.EpochEndOffset

/**
 *  抽象类用于从同一代理的多个分区中获取数据。
 */
abstract class AbstractFetcherThread(name: String,
                                     clientId: String,
                                     val sourceBroker: BrokerEndPoint,
                                     fetchBackOffMs: Int = 0,
                                     isInterruptible: Boolean = true,
                                     includeLogTruncation: Boolean)
  extends ShutdownableThread(name, isInterruptible) {

  type REQ <: FetchRequest
  type PD <: PartitionData

  private[server] val partitionStates = new PartitionStates[PartitionFetchState]
  private val partitionMapLock = new ReentrantLock
  private val partitionMapCond = partitionMapLock.newCondition()

  private val metricId = ClientIdAndBroker(clientId, sourceBroker.host, sourceBroker.port)
  val fetcherStats = new FetcherStats(metricId)
  val fetcherLagStats = new FetcherLagStats(metricId)

  /* 子类中定义的回调函数 */

  // process fetched data
  protected def processPartitionData(topicPartition: TopicPartition, fetchOffset: Long, partitionData: PD)

  // 处理偏移量超出范围的分区并返回新的提取偏移量
  protected def handleOffsetOutOfRange(topicPartition: TopicPartition): Long

  // 处理有错误的分区，这可能是由于领导层的变化
  protected def handlePartitionsWithErrors(partitions: Iterable[TopicPartition])

  protected def buildLeaderEpochRequest(allPartitions: Seq[(TopicPartition, PartitionFetchState)]): ResultWithPartitions[Map[TopicPartition, Int]]

  protected def fetchEpochsFromLeader(partitions: Map[TopicPartition, Int]): Map[TopicPartition, EpochEndOffset]

  protected def maybeTruncate(fetchedEpochs: Map[TopicPartition, EpochEndOffset]): ResultWithPartitions[Map[TopicPartition, Long]]

  protected def buildFetchRequest(partitionMap: Seq[(TopicPartition, PartitionFetchState)]): ResultWithPartitions[REQ]

  protected def fetch(fetchRequest: REQ): Seq[(TopicPartition, PD)]

  override def shutdown() {
    initiateShutdown()
    inLock(partitionMapLock) {
      partitionMapCond.signalAll()
    }
    awaitShutdown()

    // 由于线程已完成关闭并且度量标准移除是安全的，因此我们不需要锁定
    fetcherStats.unregister()
    fetcherLagStats.unregister()
  }

  private def states() = partitionStates.partitionStates.asScala.map { state => state.topicPartition -> state.value }

  override def doWork() {
    maybeTruncate()
    val fetchRequest = inLock(partitionMapLock) {
      val ResultWithPartitions(fetchRequest, partitionsWithError) = buildFetchRequest(states)
      if (fetchRequest.isEmpty) {
        trace(s"There are no active partitions. Back off for $fetchBackOffMs ms before sending a fetch request")
        partitionMapCond.await(fetchBackOffMs, TimeUnit.MILLISECONDS)
      }
      handlePartitionsWithErrors(partitionsWithError)
      fetchRequest
    }
    if (!fetchRequest.isEmpty)
      processFetchRequest(fetchRequest) // 处理fetch请求
  }

  /**
    * - Build a leader epoch fetch based on partitions that are in the Truncating phase
    * - Issue LeaderEpochRequeust, retrieving the latest offset for each partition's
    *   leader epoch. This is the offset the follower should truncate to ensure
    *   accurate log replication.
    * - Finally truncate the logs for partitions in the truncating phase and mark them
    *   truncation complete. Do this within a lock to ensure no leadership changes can
    *   occur during truncation.
    * - 基于处于截断阶段的分区构建领导者时期提取
    * - 发布LeaderEpochRequeust，检索每个分区的最新偏移量
  *   领袖时代。 这是追随者应该截断以确保的偏移量
  *   准确的日志复制。
    * - 最后在截断阶段截断分区的日志并标记它们
  *   截断完成。 在锁定下做到这一点，以确保没有领导层的变化
  *   在截断期间发生。
    */
  def maybeTruncate(): Unit = {
    val ResultWithPartitions(epochRequests, partitionsWithError) = inLock(partitionMapLock) { buildLeaderEpochRequest(states) }
    handlePartitionsWithErrors(partitionsWithError) // 处理错误的分区

    if (epochRequests.nonEmpty) {
      val fetchedEpochs = fetchEpochsFromLeader(epochRequests) // 根据epoch从领导者获取分区信息
      // 确保我们在处理事务时，持有锁
      inLock(partitionMapLock) {
        // 检查我们解锁时没有发生领导变化，取得epoch
        val leaderEpochs = fetchedEpochs.filter { case (tp, _) => partitionStates.contains(tp) }
        val ResultWithPartitions(fetchOffsets, partitionsWithError) = maybeTruncate(leaderEpochs) // 根据epoch截取partiton
        handlePartitionsWithErrors(partitionsWithError) // 继续处理错误partioton
        markTruncationCompleteAndUpdateFetchOffset(fetchOffsets) // 标记事务完成，并更新分区状态
      }
    }
  }

  // 发送并处理fetch请求，并处理响应
  private def processFetchRequest(fetchRequest: REQ) {
    val partitionsWithError = mutable.Set[TopicPartition]()
    var responseData: Seq[(TopicPartition, PD)] = Seq.empty

    try {
      trace(s"Sending fetch request $fetchRequest")
      responseData = fetch(fetchRequest)// 发送请求
    } catch {
      case t: Throwable =>
        if (isRunning) {
          warn(s"Error in response for fetch request $fetchRequest", t)
          inLock(partitionMapLock) {
            partitionsWithError ++= partitionStates.partitionSet.asScala
            // there is an error occurred while fetching partitions, sleep a while
            // note that `ReplicaFetcherThread.handlePartitionsWithError` will also introduce the same delay for every
            // partition with error effectively doubling the delay. It would be good to improve this.
            partitionMapCond.await(fetchBackOffMs, TimeUnit.MILLISECONDS)
          }
        }
    }
    fetcherStats.requestRate.mark() // 标记速率

    if (responseData.nonEmpty) {
      // process fetched data
      inLock(partitionMapLock) {

        responseData.foreach { case (topicPartition, partitionData) =>
          val topic = topicPartition.topic
          val partitionId = topicPartition.partition
          Option(partitionStates.stateValue(topicPartition)).foreach(currentPartitionFetchState => // 当前分区的fetch状态
            // 当存在挂起的获取请求时，可能会删除分区并重新添加或截断分区。
            // 在这种情况下，如果分区状态准备好提取并且当前偏移量与请求的偏移量相同，我们只想处理获取响应。
            if (fetchRequest.offset(topicPartition) == currentPartitionFetchState.fetchOffset &&
                currentPartitionFetchState.isReadyForFetch) {
              partitionData.error match {
                case Errors.NONE =>
                  try {
                    val records = partitionData.toRecords // 分区的消息记录数据
                    val newOffset = records.batches.asScala.lastOption.map(_.nextOffset).getOrElse(
                      currentPartitionFetchState.fetchOffset) // 分区最新的偏移量

                    // 计算落后的偏移量
                    fetcherLagStats.getAndMaybePut(topic, partitionId).lag = Math.max(0L, partitionData.highWatermark - newOffset)
                    // 一旦我们把分区数据交给子类，我们就不会再在这个线程中弄乱它了。
                    processPartitionData(topicPartition, currentPartitionFetchState.fetchOffset, partitionData)

                    val validBytes = records.validBytes
                    // 在处理分区数据后，ReplicaDirAlterThread可能已从partitionStates中删除了topicPartition
                    if (validBytes > 0 && partitionStates.contains(topicPartition)) {
                      // 只有在processPartitionData期间没有异常时才更新partitionState
                      partitionStates.updateAndMoveToEnd(topicPartition, new PartitionFetchState(newOffset))
                      fetcherStats.byteRate.mark(validBytes)
                    }
                  } catch {
                    case ime: CorruptRecordException =>
                      // we log the error and continue. This ensures two things
                      // 1. If there is a corrupt message in a topic partition, it does not bring the fetcher thread down and cause other topic partition to also lag
                      // 2. If the message is corrupt due to a transient state in the log (truncation, partial writes can cause this), we simply continue and
                      // should get fixed in the subsequent fetches
                      error(s"Found invalid messages during fetch for partition $topicPartition offset ${currentPartitionFetchState.fetchOffset}", ime)
                      partitionsWithError += topicPartition
                    case e: KafkaStorageException =>
                      error(s"Error while processing data for partition $topicPartition", e)
                      partitionsWithError += topicPartition
                    case e: Throwable =>
                      throw new KafkaException(s"Error processing data for partition $topicPartition " +
                        s"offset ${currentPartitionFetchState.fetchOffset}", e)
                  }
                case Errors.OFFSET_OUT_OF_RANGE =>
                  try {
                    val newOffset = handleOffsetOutOfRange(topicPartition)
                    partitionStates.updateAndMoveToEnd(topicPartition, new PartitionFetchState(newOffset))
                    info(s"Current offset ${currentPartitionFetchState.fetchOffset} for partition $topicPartition is " +
                      s"out of range, which typically implies a leader change. Reset fetch offset to $newOffset")
                  } catch {
                    case e: FatalExitError => throw e
                    case e: Throwable =>
                      error(s"Error getting offset for partition $topicPartition", e)
                      partitionsWithError += topicPartition
                  }

                case Errors.NOT_LEADER_FOR_PARTITION =>
                  info(s"Remote broker is not the leader for partition $topicPartition, which could indicate " +
                    "that the partition is being moved")
                  partitionsWithError += topicPartition

                case _ =>
                  error(s"Error for partition $topicPartition at offset ${currentPartitionFetchState.fetchOffset}",
                    partitionData.exception.get)
                  partitionsWithError += topicPartition
              }
            })
        }
      }
    }

    if (partitionsWithError.nonEmpty) {
      debug(s"Handling errors for partitions $partitionsWithError")
      handlePartitionsWithErrors(partitionsWithError)
    }
  }

  // 更新tp的partitionStates（事务）
  def markPartitionsForTruncation(topicPartition: TopicPartition, truncationOffset: Long) {
    if (!includeLogTruncation)
      throw new IllegalStateException("Truncation should not be requested if includeLogTruncation is disabled")
    partitionMapLock.lockInterruptibly()
    try {
      Option(partitionStates.stateValue(topicPartition)).foreach { state => // 设置事务状态的偏移量
        val newState = PartitionFetchState(math.min(truncationOffset, state.fetchOffset), state.delay, truncatingLog = true)
        partitionStates.updateAndMoveToEnd(topicPartition, newState)
      }
      partitionMapCond.signalAll()
    } finally partitionMapLock.unlock()
  }

  def addPartitions(initialFetchOffsets: Map[TopicPartition, Long]) {
    partitionMapLock.lockInterruptibly()
    try {
      // If the partitionMap already has the topic/partition, then do not update the map with the old offset
      // 如果partitionMap已经有主题/分区，则不要更新旧的偏移map
      val newPartitionToState = initialFetchOffsets.filter { case (tp, _) =>
        !partitionStates.contains(tp)
      }.map { case (tp, initialFetchOffset) =>
        val fetchState =
          if (initialFetchOffset < 0)
            new PartitionFetchState(handleOffsetOutOfRange(tp), includeLogTruncation)
          else
            new PartitionFetchState(initialFetchOffset, includeLogTruncation)
        tp -> fetchState
      }
      val existingPartitionToState = states().toMap
      partitionStates.set((existingPartitionToState ++ newPartitionToState).asJava)
      partitionMapCond.signalAll()
    } finally partitionMapLock.unlock()
  }

  /**
    * 遍历所有分区，将其标记为截断完成并更新提取偏移量
    *
    * @param fetchOffsets the partitions to mark truncation complete
    */
  private def markTruncationCompleteAndUpdateFetchOffset(fetchOffsets: Map[TopicPartition, Long]) {
    val newStates: Map[TopicPartition, PartitionFetchState] = partitionStates.partitionStates.asScala
      .map { state => // 可能截取的完成事务状态
        val maybeTruncationComplete = fetchOffsets.get(state.topicPartition()) match {
          case Some(offset) => PartitionFetchState(offset, state.value.delay, truncatingLog = false)
          case None => state.value()
        }
        (state.topicPartition(), maybeTruncationComplete)
      }.toMap
    partitionStates.set(newStates.asJava) // 清空，之后更新新的分区状态
  }

  // 延迟分区的处理
  def delayPartitions(partitions: Iterable[TopicPartition], delay: Long) {
    partitionMapLock.lockInterruptibly() // 立即获取锁
    try {
      for (partition <- partitions) {
        Option(partitionStates.stateValue(partition)).foreach(currentPartitionFetchState =>
          if (!currentPartitionFetchState.isDelayed)// 进行延迟
            partitionStates.updateAndMoveToEnd(partition, PartitionFetchState(currentPartitionFetchState.fetchOffset, new DelayedItem(delay), currentPartitionFetchState.truncatingLog))
        )
      }
      partitionMapCond.signalAll()
    } finally partitionMapLock.unlock()
  }

  def removePartitions(topicPartitions: Set[TopicPartition]) {
    partitionMapLock.lockInterruptibly()
    try {
      topicPartitions.foreach { topicPartition =>
        partitionStates.remove(topicPartition) // 移除分区状态
        fetcherLagStats.unregister(topicPartition.topic, topicPartition.partition) // 注销fetcher的延迟状态
      }
    } finally partitionMapLock.unlock()
  }

  def partitionCount() = {
    partitionMapLock.lockInterruptibly()
    try partitionStates.size
    finally partitionMapLock.unlock()
  }

  private[server] def partitionsAndOffsets: Map[TopicPartition, BrokerAndInitialOffset] = inLock(partitionMapLock) {
    partitionStates.partitionStates.asScala.map { case state =>
      state.topicPartition -> new BrokerAndInitialOffset(sourceBroker, state.value.fetchOffset)
    }.toMap
  }

}

object AbstractFetcherThread {

  // 关于分区的结果，分两部分：获取到分区结果的和未获取到分区结果的
  case class ResultWithPartitions[R](result: R, partitionsWithError: Set[TopicPartition])

  trait FetchRequest {
    def isEmpty: Boolean
    def offset(topicPartition: TopicPartition): Long
  }

  trait PartitionData {
    def error: Errors
    def exception: Option[Throwable]
    def toRecords: MemoryRecords
    def highWatermark: Long
  }

}

object FetcherMetrics {
  val ConsumerLag = "ConsumerLag"
  val RequestsPerSec = "RequestsPerSec"
  val BytesPerSec = "BytesPerSec"
}

// feacher的落后度量
class FetcherLagMetrics(metricId: ClientIdTopicPartition) extends KafkaMetricsGroup {

  // 落后数
  private[this] val lagVal = new AtomicLong(-1L)
  private[this] val tags = Map(
    "clientId" -> metricId.clientId,
    "topic" -> metricId.topic,
    "partition" -> metricId.partitionId.toString)

  newGauge(FetcherMetrics.ConsumerLag,
    new Gauge[Long] {
      def value = lagVal.get
    },
    tags
  )

  def lag_=(newLag: Long) {
    lagVal.set(newLag)
  }

  def lag = lagVal.get

  def unregister() {
    removeMetric(FetcherMetrics.ConsumerLag, tags)
  }
}

// fetch的落后状态
class FetcherLagStats(metricId: ClientIdAndBroker) {
  private val valueFactory = (k: ClientIdTopicPartition) => new FetcherLagMetrics(k)
  val stats = new Pool[ClientIdTopicPartition, FetcherLagMetrics](Some(valueFactory))

  def getAndMaybePut(topic: String, partitionId: Int): FetcherLagMetrics = {
    stats.getAndMaybePut(ClientIdTopicPartition(metricId.clientId, topic, partitionId))
  }

  // 判断副本是否在同步中
  def isReplicaInSync(topic: String, partitionId: Int): Boolean = {
    val fetcherLagMetrics = stats.get(ClientIdTopicPartition(metricId.clientId, topic, partitionId))
    if (fetcherLagMetrics != null)
      fetcherLagMetrics.lag <= 0
    else
      false
  }

  def unregister(topic: String, partitionId: Int) {
    val lagMetrics = stats.remove(ClientIdTopicPartition(metricId.clientId, topic, partitionId))
    if (lagMetrics != null) lagMetrics.unregister()
  }

  def unregister() {
    stats.keys.toBuffer.foreach { key: ClientIdTopicPartition =>
      unregister(key.topic, key.partitionId)
    }
  }
}

class FetcherStats(metricId: ClientIdAndBroker) extends KafkaMetricsGroup {
  val tags = Map("clientId" -> metricId.clientId,
    "brokerHost" -> metricId.brokerHost,
    "brokerPort" -> metricId.brokerPort.toString)

  val requestRate = newMeter(FetcherMetrics.RequestsPerSec, "requests", TimeUnit.SECONDS, tags)

  val byteRate = newMeter(FetcherMetrics.BytesPerSec, "bytes", TimeUnit.SECONDS, tags)

  def unregister() {
    removeMetric(FetcherMetrics.RequestsPerSec, tags)
    removeMetric(FetcherMetrics.BytesPerSec, tags)
  }

}

case class ClientIdTopicPartition(clientId: String, topic: String, partitionId: Int) {
  override def toString = "%s-%s-%d".format(clientId, topic, partitionId)
}

/**
  * 类保持分区偏移量及其状态（truncatingLog，延迟）
  * 这表示分区是以下两种情况之一：
  * （1）截断其日志，例如最近成为追随者
  *（2）延迟，例如由于错误，我们随后退后一段时间
  *（3）isReadyForFetch，这是线程主动获取数据的活动状态。
  */
case class PartitionFetchState(fetchOffset: Long, delay: DelayedItem, truncatingLog: Boolean = false) {

  def this(offset: Long, truncatingLog: Boolean) = this(offset, new DelayedItem(0), truncatingLog)

  def this(offset: Long, delay: DelayedItem) = this(offset, new DelayedItem(0), false)

  def this(fetchOffset: Long) = this(fetchOffset, new DelayedItem(0))

  def isReadyForFetch: Boolean = delay.getDelay(TimeUnit.MILLISECONDS) == 0 && !truncatingLog

  def isTruncatingLog: Boolean = delay.getDelay(TimeUnit.MILLISECONDS) == 0 && truncatingLog

  def isDelayed: Boolean = delay.getDelay(TimeUnit.MILLISECONDS) > 0

  override def toString = "offset:%d-isReadyForFetch:%b-isTruncatingLog:%b".format(fetchOffset, isReadyForFetch, truncatingLog)
}
