/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.server

import kafka.server.QuotaType._
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Time

// 配额的种类
object QuotaType  {
  case object Fetch extends QuotaType
  case object Produce extends QuotaType
  case object Request extends QuotaType
  case object LeaderReplication extends QuotaType
  case object FollowerReplication extends QuotaType
  case object AlterLogDirsReplication extends QuotaType
}
sealed trait QuotaType

// 配额工厂用于管理限制集群中元素的速度
object QuotaFactory extends Logging {

  // 无界限的配置
  object UnboundedQuota extends ReplicaQuota {
    override def isThrottled(topicPartition: TopicPartition): Boolean = false
    override def isQuotaExceeded(): Boolean = false
  }

  case class QuotaManagers(fetch: ClientQuotaManager,
                           produce: ClientQuotaManager,
                           request: ClientRequestQuotaManager,
                           leader: ReplicationQuotaManager,
                           follower: ReplicationQuotaManager,
                           alterLogDirs: ReplicationQuotaManager) {
    def shutdown() {
      fetch.shutdown
      produce.shutdown
      request.shutdown
    }
  }

  // 初始化配额管理器
  def instantiate(cfg: KafkaConfig, metrics: Metrics, time: Time, threadNamePrefix: String): QuotaManagers = {
    QuotaManagers(
      new ClientQuotaManager(clientFetchConfig(cfg), metrics, Fetch, time, threadNamePrefix),
      new ClientQuotaManager(clientProduceConfig(cfg), metrics, Produce, time, threadNamePrefix),
      new ClientRequestQuotaManager(clientRequestConfig(cfg), metrics, time, threadNamePrefix),
      new ReplicationQuotaManager(replicationConfig(cfg), metrics, LeaderReplication, time),
      new ReplicationQuotaManager(replicationConfig(cfg), metrics, FollowerReplication, time),
      new ReplicationQuotaManager(alterLogDirsReplicationConfig(cfg), metrics, AlterLogDirsReplication, time)
    )
  }

  // 关于生产的配额的配置文件
  def clientProduceConfig(cfg: KafkaConfig): ClientQuotaManagerConfig = {
    if (cfg.producerQuotaBytesPerSecondDefault != Long.MaxValue)
      warn(s"${KafkaConfig.ProducerQuotaBytesPerSecondDefaultProp} has been deprecated in 0.11.0.0 and will be removed in a future release. Use dynamic quota defaults instead.")
    ClientQuotaManagerConfig(
      quotaBytesPerSecondDefault = cfg.producerQuotaBytesPerSecondDefault,
      numQuotaSamples = cfg.numQuotaSamples,
      quotaWindowSizeSeconds = cfg.quotaWindowSizeSeconds
    )
  }

  // 关于获取的配额的配置文件
  def clientFetchConfig(cfg: KafkaConfig): ClientQuotaManagerConfig = {
    if (cfg.consumerQuotaBytesPerSecondDefault != Long.MaxValue)
      warn(s"${KafkaConfig.ConsumerQuotaBytesPerSecondDefaultProp} has been deprecated in 0.11.0.0 and will be removed in a future release. Use dynamic quota defaults instead.")
    ClientQuotaManagerConfig(
      quotaBytesPerSecondDefault = cfg.consumerQuotaBytesPerSecondDefault,
      numQuotaSamples = cfg.numQuotaSamples,
      quotaWindowSizeSeconds = cfg.quotaWindowSizeSeconds
    )
  }

  // 关于客户端请求配额的配置文件
  def clientRequestConfig(cfg: KafkaConfig): ClientQuotaManagerConfig = {
    ClientQuotaManagerConfig(
      numQuotaSamples = cfg.numQuotaSamples,
      quotaWindowSizeSeconds = cfg.quotaWindowSizeSeconds
    )
  }

  def replicationConfig(cfg: KafkaConfig): ReplicationQuotaManagerConfig = {
    ReplicationQuotaManagerConfig(
      numQuotaSamples = cfg.numReplicationQuotaSamples,
      quotaWindowSizeSeconds = cfg.replicationQuotaWindowSizeSeconds
    )
  }

  def alterLogDirsReplicationConfig(cfg: KafkaConfig): ReplicationQuotaManagerConfig = {
    ReplicationQuotaManagerConfig(
      numQuotaSamples = cfg.numAlterLogDirsReplicationQuotaSamples,
      quotaWindowSizeSeconds = cfg.alterLogDirsReplicationQuotaWindowSizeSeconds
    )
  }

}
