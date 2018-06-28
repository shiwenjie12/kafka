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

import java.util.concurrent.TimeUnit

import kafka.metrics.KafkaMetricsGroup
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{NotLeaderForPartitionException, UnknownTopicOrPartitionException, KafkaStorageException}
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.requests.IsolationLevel

import scala.collection._

case class FetchPartitionStatus(startOffsetMetadata: LogOffsetMetadata, fetchInfo: PartitionData) {

  override def toString = "[startOffsetMetadata: " + startOffsetMetadata + ", " +
                          "fetchInfo: " + fetchInfo + "]"
}

/**
 * The fetch metadata maintained by the delayed fetch operation
 */
case class FetchMetadata(fetchMinBytes: Int,
                         fetchMaxBytes: Int,
                         hardMaxBytesLimit: Boolean,
                         fetchOnlyLeader: Boolean,
                         fetchOnlyCommitted: Boolean,
                         isFromFollower: Boolean,
                         replicaId: Int,
                         fetchPartitionStatus: Seq[(TopicPartition, FetchPartitionStatus)]) {

  override def toString = "[minBytes: " + fetchMinBytes + ", " +
    "maxBytes:" + fetchMaxBytes + ", " +
    "onlyLeader:" + fetchOnlyLeader + ", " +
    "onlyCommitted: " + fetchOnlyCommitted + ", " +
    "replicaId: " + replicaId + ", " +
    "partitionStatus: " + fetchPartitionStatus + "]"
}
/**
  * 延迟获取操作，可由副本管理器创建并在提取操作中进行监视
 */
class DelayedFetch(delayMs: Long,
                   fetchMetadata: FetchMetadata,
                   replicaManager: ReplicaManager,
                   quota: ReplicaQuota,
                   isolationLevel: IsolationLevel,
                   responseCallback: Seq[(TopicPartition, FetchPartitionData)] => Unit)
  extends DelayedOperation(delayMs) {

  /**
   * 在以下情况下可以完成操作：
   *
   * 案例A：这个broker不再是它试图获取某些分区的领导者
   * 案例B：此代理不知道它尝试获取的某些分区
   * 情况C：提取偏移量不在日志的最后一段
   * 情况D：来自所有读取分区的累计字节超过最小字节数s
   * 情况E：该分区位于该代理上的脱机日志目录中
   *
   * 完成后，应返回每个有效分区的可用数据
   */
  override def tryComplete() : Boolean = {
    var accumulatedSize = 0
    var accumulatedThrottledSize = 0
    fetchMetadata.fetchPartitionStatus.foreach {
      case (topicPartition, fetchStatus) =>
        val fetchOffset = fetchStatus.startOffsetMetadata
        try {
          if (fetchOffset != LogOffsetMetadata.UnknownOffsetMetadata) {
            val replica = replicaManager.getLeaderReplicaIfLocal(topicPartition)
            val endOffset =
              if (isolationLevel == IsolationLevel.READ_COMMITTED)
                replica.lastStableOffset
              else if (fetchMetadata.fetchOnlyCommitted)
                replica.highWatermark
              else
                replica.logEndOffset

            // 如果消息偏移量相同，则直接检查案例D. 如果日志段刚刚滚动，则高水印偏移量将保持不变，
            // 但位于旧段上，这会错误地视为情况C的一个实例。
            if (endOffset.messageOffset != fetchOffset.messageOffset) {
              if (endOffset.onOlderSegment(fetchOffset)) {
                // Case C, 当新的提取操作位于截断的领导者上时，可能会发生这种情况
                debug("Satisfying fetch %s since it is fetching later segments of partition %s.".format(fetchMetadata, topicPartition))
                return forceComplete()
              } else if (fetchOffset.onOlderSegment(endOffset)) {
                // Case C, 当提取操作落在当前分段的后面或分区刚刚推出新分段时，可能会发生这种情况
                debug("Satisfying fetch %s immediately since it is fetching older segments.".format(fetchMetadata))
                // 如果副本应该被限制，我们不会强制完成获取请求。
                if (!replicaManager.shouldLeaderThrottle(quota, topicPartition, fetchMetadata.replicaId))
                  return forceComplete()
              } else if (fetchOffset.messageOffset < endOffset.messageOffset) {
                // 当累积字节时我们将分区读取大小作为上限（如果是一个受限分区，则跳过）
                val bytesAvailable = math.min(endOffset.positionDiff(fetchOffset), fetchStatus.fetchInfo.maxBytes)
                if (quota.isThrottled(topicPartition))
                  accumulatedThrottledSize += bytesAvailable
                else
                  accumulatedSize += bytesAvailable
              }
            }
          }
        } catch {
          case _: KafkaStorageException => // Case E
            debug("Partition %s is in an offline log directory, satisfy %s immediately".format(topicPartition, fetchMetadata))
            return forceComplete()
          case _: UnknownTopicOrPartitionException => // Case B
            debug("Broker no longer know of %s, satisfy %s immediately".format(topicPartition, fetchMetadata))
            return forceComplete()
          case _: NotLeaderForPartitionException =>  // Case A
            debug("Broker is no longer the leader of %s, satisfy %s immediately".format(topicPartition, fetchMetadata))
            return forceComplete()
        }
    }

    // Case D
    if (accumulatedSize >= fetchMetadata.fetchMinBytes
      || ((accumulatedSize + accumulatedThrottledSize) >= fetchMetadata.fetchMinBytes && !quota.isQuotaExceeded()))
      forceComplete()
    else
      false
  }

  override def onExpiration() {
    if (fetchMetadata.isFromFollower)
      DelayedFetchMetrics.followerExpiredRequestMeter.mark()
    else
      DelayedFetchMetrics.consumerExpiredRequestMeter.mark()
  }

  /**
   * Upon completion, read whatever data is available and pass to the complete callback
   */
  override def onComplete() {
    val logReadResults = replicaManager.readFromLocalLog(
      replicaId = fetchMetadata.replicaId,
      fetchOnlyFromLeader = fetchMetadata.fetchOnlyLeader,
      readOnlyCommitted = fetchMetadata.fetchOnlyCommitted,
      fetchMaxBytes = fetchMetadata.fetchMaxBytes,
      hardMaxBytesLimit = fetchMetadata.hardMaxBytesLimit,
      readPartitionInfo = fetchMetadata.fetchPartitionStatus.map { case (tp, status) => tp -> status.fetchInfo },
      quota = quota,
      isolationLevel = isolationLevel)

    val fetchPartitionData = logReadResults.map { case (tp, result) =>
      tp -> FetchPartitionData(result.error, result.highWatermark, result.leaderLogStartOffset, result.info.records,
        result.lastStableOffset, result.info.abortedTransactions)
    }

    responseCallback(fetchPartitionData)
  }
}

object DelayedFetchMetrics extends KafkaMetricsGroup {
  private val FetcherTypeKey = "fetcherType"
  val followerExpiredRequestMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS, tags = Map(FetcherTypeKey -> "follower"))
  val consumerExpiredRequestMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS, tags = Map(FetcherTypeKey -> "consumer"))
}

