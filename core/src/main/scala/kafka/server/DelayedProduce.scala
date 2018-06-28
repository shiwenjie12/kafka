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
import java.util.concurrent.locks.Lock

import com.yammer.metrics.core.Meter
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.Pool

import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse

import scala.collection._

case class ProducePartitionStatus(requiredOffset: Long, responseStatus: PartitionResponse) {
  @volatile var acksPending = false

  override def toString = "[acksPending: %b, error: %d, startOffset: %d, requiredOffset: %d]"
    .format(acksPending, responseStatus.error.code, responseStatus.baseOffset, requiredOffset)
}

/**
 * 延迟生产操作维护的元数据
 */
case class ProduceMetadata(produceRequiredAcks: Short,
                           produceStatus: Map[TopicPartition, ProducePartitionStatus]) {

  override def toString = "[requiredAcks: %d, partitionStatus: %s]"
    .format(produceRequiredAcks, produceStatus)
}

/**
  * 一个延迟的生产操作，可以由副本管理器创建并在生产操作炼狱（purgatory）中观察
 */
class DelayedProduce(delayMs: Long,
                     produceMetadata: ProduceMetadata,
                     replicaManager: ReplicaManager,
                     responseCallback: Map[TopicPartition, PartitionResponse] => Unit,
                     lockOpt: Option[Lock] = None)
  extends DelayedOperation(delayMs, lockOpt) {

  // 首先根据错误代码更新ACK挂起变量
  produceMetadata.produceStatus.foreach { case (topicPartition, status) =>
    if (status.responseStatus.error == Errors.NONE) {
      // 当接收到需要的ACK时，将清除超时错误状态。
      status.acksPending = true
      status.responseStatus.error = Errors.REQUEST_TIMED_OUT// 初始化请求超时
    } else { // 有错误则不需要等待
      status.acksPending = false
    }

    trace("Initial partition status for %s is %s".format(topicPartition, status))
  }

  /**
   * 如果生成的每个分区都满足下列延迟之一，则可以完成延迟生成操作：
   * Case A: This broker is no longer the leader: set an error in response
   * Case B: This broker is the leader:
   *   B.1 - If there was a local error thrown while checking if at least requiredAcks
   *         replicas have caught up to this operation: set an error in response
   *   B.2 - Otherwise, set the response with no error.
   */
  override def tryComplete(): Boolean = {
    // check for each partition if it still has pending acks
    produceMetadata.produceStatus.foreach { case (topicPartition, status) =>
      trace(s"Checking produce satisfaction for $topicPartition, current status $status")
      // 跳过那些已经满足的分区
      if (status.acksPending) {
        val (hasEnough, error) = replicaManager.getPartition(topicPartition) match {
          case Some(partition) =>
            if (partition eq ReplicaManager.OfflinePartition)  // 下线的分区
              (false, Errors.KAFKA_STORAGE_ERROR)
            else
              partition.checkEnoughReplicasReachOffset(status.requiredOffset) // 检查是否有足够的副本到达指定的偏移量
          case None =>
            // Case A
            (false, Errors.UNKNOWN_TOPIC_OR_PARTITION)  // 未找的的分区
        }
        // Case B.1 || B.2
        if (error != Errors.NONE || hasEnough) {
          status.acksPending = false
          status.responseStatus.error = error
        }
      }
    }

    // check if every partition has satisfied at least one of case A or B
    if (!produceMetadata.produceStatus.values.exists(_.acksPending))
      forceComplete()
    else
      false
  }

  override def onExpiration() {
    produceMetadata.produceStatus.foreach { case (topicPartition, status) =>
      if (status.acksPending) {
        DelayedProduceMetrics.recordExpiration(topicPartition)
      }
    }
  }

  /**
   * 完成后，将当前响应状态连同每个分区的错误代码一起返回。
   */
  override def onComplete() {
    val responseStatus = produceMetadata.produceStatus.mapValues(status => status.responseStatus)
    responseCallback(responseStatus) // 回调响应
  }
}

object DelayedProduceMetrics extends KafkaMetricsGroup {

  private val aggregateExpirationMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS)

  private val partitionExpirationMeterFactory = (key: TopicPartition) =>
    newMeter("ExpiresPerSec",
             "requests",
             TimeUnit.SECONDS,
             tags = Map("topic" -> key.topic, "partition" -> key.partition.toString))
  private val partitionExpirationMeters = new Pool[TopicPartition, Meter](valueFactory = Some(partitionExpirationMeterFactory))

  def recordExpiration(partition: TopicPartition) {
    aggregateExpirationMeter.mark()
    partitionExpirationMeters.getAndMaybePut(partition).mark()
  }
}

