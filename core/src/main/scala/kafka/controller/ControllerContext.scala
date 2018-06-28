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

package kafka.controller

import kafka.cluster.Broker
import org.apache.kafka.common.TopicPartition

import scala.collection.{Seq, Set, mutable}

// 控制器的上下文
class ControllerContext {
  val stats = new ControllerStats

  var controllerChannelManager: ControllerChannelManager = null

  // 正在关闭broker
  var shuttingDownBrokerIds: mutable.Set[Int] = mutable.Set.empty
  var epoch: Int = KafkaController.InitialControllerEpoch - 1
  var epochZkVersion: Int = KafkaController.InitialControllerEpochZkVersion - 1
  var allTopics: Set[String] = Set.empty
  // 分配的副本分区
  var partitionReplicaAssignment: mutable.Map[TopicPartition, Seq[Int]] = mutable.Map.empty
  // tp的领导中存储的信息
  var partitionLeadershipInfo: mutable.Map[TopicPartition, LeaderIsrAndControllerEpoch] = mutable.Map.empty
  // 正在分配的分区
  val partitionsBeingReassigned: mutable.Map[TopicPartition, ReassignedPartitionsContext] = mutable.Map.empty
  val replicasOnOfflineDirs: mutable.Map[Int, Set[TopicPartition]] = mutable.Map.empty

  // 潜在存活的broker，可能包含关闭中的broker
  private var liveBrokersUnderlying: Set[Broker] = Set.empty
  private var liveBrokerIdsUnderlying: Set[Int] = Set.empty

  // setter
  def liveBrokers_=(brokers: Set[Broker]) {
    liveBrokersUnderlying = brokers
    liveBrokerIdsUnderlying = liveBrokersUnderlying.map(_.id)
  }

  // 存活的broker
  def liveBrokers = liveBrokersUnderlying.filter(broker => !shuttingDownBrokerIds.contains(broker.id))
  def liveBrokerIds = liveBrokerIdsUnderlying -- shuttingDownBrokerIds

  // 存活的或者正在关闭的broker
  def liveOrShuttingDownBrokerIds = liveBrokerIdsUnderlying
  def liveOrShuttingDownBrokers = liveBrokersUnderlying

  def partitionsOnBroker(brokerId: Int): Set[TopicPartition] = {
    partitionReplicaAssignment.collect {
      case (topicPartition, replicas) if replicas.contains(brokerId) => topicPartition
    }.toSet
  }

  // 判断 tp在某个broker上的副本是否在线
  def isReplicaOnline(brokerId: Int, topicPartition: TopicPartition, includeShuttingDownBrokers: Boolean = false): Boolean = {
    val brokerOnline = { // broker是否在线
      if (includeShuttingDownBrokers) liveOrShuttingDownBrokerIds.contains(brokerId)
      else liveBrokerIds.contains(brokerId)
    }
    brokerOnline && !replicasOnOfflineDirs.getOrElse(brokerId, Set.empty).contains(topicPartition)
  }

  def replicasOnBrokers(brokerIds: Set[Int]): Set[PartitionAndReplica] = {
    brokerIds.flatMap { brokerId =>
      partitionReplicaAssignment.collect { case (topicPartition, replicas) if replicas.contains(brokerId) =>
        PartitionAndReplica(topicPartition, brokerId)
      }
    }.toSet
  }

  def replicasForTopic(topic: String): Set[PartitionAndReplica] = {
    partitionReplicaAssignment
      .filter { case (topicPartition, _) => topicPartition.topic == topic }
      .flatMap { case (topicPartition, replicas) =>
        replicas.map(PartitionAndReplica(topicPartition, _))
      }.toSet
  }

  def partitionsForTopic(topic: String): collection.Set[TopicPartition] =
    partitionReplicaAssignment.keySet.filter(topicPartition => topicPartition.topic == topic)

  def allLiveReplicas(): Set[PartitionAndReplica] = {
    replicasOnBrokers(liveBrokerIds).filter { partitionAndReplica =>
      isReplicaOnline(partitionAndReplica.replica, partitionAndReplica.topicPartition)
    }
  }

  def replicasForPartition(partitions: collection.Set[TopicPartition]): collection.Set[PartitionAndReplica] = {
    partitions.flatMap { p =>
      val replicas = partitionReplicaAssignment(p)
      replicas.map(PartitionAndReplica(p, _))
    }
  }

  def removeTopic(topic: String) = {
    partitionLeadershipInfo = partitionLeadershipInfo.filter { case (topicPartition, _) => topicPartition.topic != topic }
    partitionReplicaAssignment = partitionReplicaAssignment.filter { case (topicPartition, _) => topicPartition.topic != topic }
    allTopics -= topic
  }

}
