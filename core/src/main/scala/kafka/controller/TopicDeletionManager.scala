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

import kafka.utils.Logging
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition

import scala.collection.{Set, mutable}

/**
 * 这管理状态机的主题删除。
 * 1. TopicCommand通过创建新的管理路径/ admin / delete_topics / <topic>来发布主题删除
 * 2.控制器在/ admin / delete_topic上侦听子级更改，并开始删除相应主题的主题cs
 * 3.控制器的ControllerEventThread处理主题删除。一个话题将不合格
 *    在以下情况下删除 -
  *   3.1经纪人托管该主题的副本之一下降
  *   3.2该分区的分区重新分配正在进行中
 * 4.当主题删除被恢复 -
 *    4.1托管该主题的副本之一的代理已启动
 *    4.2完成对该主题分区的分区重新分配
 * 5.每个被删除主题的副本都处于3个州中的任何一个 -
 *    5.1当调用onPartitionDeletion时，TopicDeletionStarted副本进入TopicDeletionStarted阶段。
 *        在控制器上触发/ admin / delete_topics的子级更改时会发生这种情况。作为这个统计的一部分ate
 *        更改，控制器将StopReplicaRequests发送到所有副本。它注册了一个回调函数
 *        当deletePartition = true时，StopReplicaResponse从而在删除副本的响应时调用回调
 *        从每个副本收到）
 *    5.2 TopicDeletion成功移动副本
 *        TopicDeletionStarted-> TopicDeletionSuccessful取决于StopReplicaResponse中的错误代码
 *    5.3 TopicDeletionFailed从中移动副本
 *        TopicDeletionStarted - > TopicDeletionFailed取决于StopReplicaResponse中的错误代码。
 *        一般来说，如果一个经纪人死亡，并且如果它为主题删除了托管副本，则控制器会标记该副本
 *        onBrokerFailure回调中的TopicDeletionFailed状态中的相应副本。原因是，如果一个
 *        代理在发送请求之前失败并且副本处于TopicDeletionStarted状态之后，
 *        复制品可能会错误地保留在TopicDeletionStarted状态和主题删除中
 *        当经纪人回来时不会重试。
 * 6.只有当所有副本都位于TopicDeletionSuccessful中时，主题才被标记为成功删除
 *    州。主题删除拆卸模式将删除controllerContext中的所有主题状态
 *    以及来自动物园管理员。这是/ brokers / topic / <topic>路径被删除的唯一时间。另一方面，,
 *    如果没有副本处于TopicDeletionStarted状态并且至少有一个副本处于TopicDeletionFailed状态，则
 *    它标志着删除重试的主题。
 * @param controller
 */
class TopicDeletionManager(controller: KafkaController,
                           eventManager: ControllerEventManager,
                           zkClient: KafkaZkClient) extends Logging {
  this.logIdent = s"[Topic Deletion Manager ${controller.config.brokerId}], "
  val controllerContext = controller.controllerContext
  val isDeleteTopicEnabled = controller.config.deleteTopicEnable
  val topicsToBeDeleted = mutable.Set.empty[String] // 用于删除的主题
  val partitionsToBeDeleted = mutable.Set.empty[TopicPartition]  // 用于删除的tp
  val topicsIneligibleForDeletion = mutable.Set.empty[String]  // 不符合删除条件的

  def init(initialTopicsToBeDeleted: Set[String], initialTopicsIneligibleForDeletion: Set[String]): Unit = {
    if (isDeleteTopicEnabled) {
      topicsToBeDeleted ++= initialTopicsToBeDeleted
      partitionsToBeDeleted ++= topicsToBeDeleted.flatMap(controllerContext.partitionsForTopic)
      topicsIneligibleForDeletion ++= initialTopicsIneligibleForDeletion & topicsToBeDeleted
    } else {
      // 如果删除主题已禁用，请清除/ admin / delete_topics下的主题条目
      info(s"Removing $initialTopicsToBeDeleted since delete topic is disabled")
      zkClient.deleteTopicDeletions(initialTopicsToBeDeleted.toSeq)
    }
  }

  def tryTopicDeletion(): Unit = {
    if (isDeleteTopicEnabled) {
      resumeDeletions()
    }
  }

  /**
   * Invoked when the current controller resigns. At this time, all state for topic deletion should be cleared.
   */
  def reset() {
    if (isDeleteTopicEnabled) {
      topicsToBeDeleted.clear()
      partitionsToBeDeleted.clear()
      topicsIneligibleForDeletion.clear()
    }
  }

  /**
   * Invoked by the child change listener on /admin/delete_topics to queue up the topics for deletion. The topic gets added
   * to the topicsToBeDeleted list and only gets removed from the list when the topic deletion has completed successfully
   * i.e. all replicas of all partitions of that topic are deleted successfully.
   * @param topics Topics that should be deleted
   */
  def enqueueTopicsForDeletion(topics: Set[String]) {
    if (isDeleteTopicEnabled) {
      topicsToBeDeleted ++= topics
      partitionsToBeDeleted ++= topics.flatMap(controllerContext.partitionsForTopic)
      resumeDeletions()
    }
  }

  /**
   * Invoked when any event that can possibly resume topic deletion occurs. These events include -
   * 1. New broker starts up. Any replicas belonging to topics queued up for deletion can be deleted since the broker is up
   * 2. Partition reassignment completes. Any partitions belonging to topics queued up for deletion finished reassignment
   * @param topics Topics for which deletion can be resumed
   */
  def resumeDeletionForTopics(topics: Set[String] = Set.empty) {
    if (isDeleteTopicEnabled) {
      val topicsToResumeDeletion = topics & topicsToBeDeleted
      if (topicsToResumeDeletion.nonEmpty) {
        topicsIneligibleForDeletion --= topicsToResumeDeletion
        resumeDeletions()
      }
    }
  }

  /**
   * Invoked when a broker that hosts replicas for topics to be deleted goes down. Also invoked when the callback for
   * StopReplicaResponse receives an error code for the replicas of a topic to be deleted. As part of this, the replicas
   * are moved from ReplicaDeletionStarted to ReplicaDeletionIneligible state. Also, the topic is added to the list of topics
   * ineligible for deletion until further notice.
   * @param replicas Replicas for which deletion has failed
   */
  def failReplicaDeletion(replicas: Set[PartitionAndReplica]) {
    if (isDeleteTopicEnabled) {
      val replicasThatFailedToDelete = replicas.filter(r => isTopicQueuedUpForDeletion(r.topic))
      if (replicasThatFailedToDelete.nonEmpty) {
        val topics = replicasThatFailedToDelete.map(_.topic)
        debug(s"Deletion failed for replicas ${replicasThatFailedToDelete.mkString(",")}. Halting deletion for topics $topics")
        // 分区标记
        controller.replicaStateMachine.handleStateChanges(replicasThatFailedToDelete.toSeq, ReplicaDeletionIneligible)
        // 主题标记
        markTopicIneligibleForDeletion(topics)
        resumeDeletions()
      }
    }
  }

  /**
   * Halt delete topic if -
   * 1. replicas being down
   * 2. partition reassignment in progress for some partitions of the topic
   * @param topics Topics that should be marked ineligible for deletion. No op if the topic is was not previously queued up for deletion
   */
  def markTopicIneligibleForDeletion(topics: Set[String]) {
    if (isDeleteTopicEnabled) {
      val newTopicsToHaltDeletion = topicsToBeDeleted & topics
      topicsIneligibleForDeletion ++= newTopicsToHaltDeletion
      if (newTopicsToHaltDeletion.nonEmpty)
        info(s"Halted deletion of topics ${newTopicsToHaltDeletion.mkString(",")}")
    }
  }

  private def isTopicIneligibleForDeletion(topic: String): Boolean = {
    if (isDeleteTopicEnabled) {
      topicsIneligibleForDeletion.contains(topic)
    } else
      true
  }

  private def isTopicDeletionInProgress(topic: String): Boolean = {
    if (isDeleteTopicEnabled) {
      controller.replicaStateMachine.isAtLeastOneReplicaInDeletionStartedState(topic)
    } else
      false
  }

  def isPartitionToBeDeleted(topicAndPartition: TopicPartition) = {
    if (isDeleteTopicEnabled) {
      partitionsToBeDeleted.contains(topicAndPartition)
    } else
      false
  }

  // 主题是否要被删除
  def isTopicQueuedUpForDeletion(topic: String): Boolean = {
    if (isDeleteTopicEnabled) {
      topicsToBeDeleted.contains(topic)
    } else
      false
  }

  /**
   * 当它没有收到要删除的主题副本的错误代码时，由StopReplicaResponse回调调用。
    * 作为其中的一部分，副本将从ReplicaDeletionStarted移动到ReplicaDeletionSuccessful状态。
    * 如果某个主题的所有副本都已成功删除，则将该主题推倒
   * @param replicas Replicas that were successfully deleted by the broker
   */
  def completeReplicaDeletion(replicas: Set[PartitionAndReplica]) {
    val successfullyDeletedReplicas = replicas.filter(r => isTopicQueuedUpForDeletion(r.topic))
    debug(s"Deletion successfully completed for replicas ${successfullyDeletedReplicas.mkString(",")}")
    controller.replicaStateMachine.handleStateChanges(successfullyDeletedReplicas.toSeq, ReplicaDeletionSuccessful)
    resumeDeletions()
  }

  /**
   * 如果 - 删除主题可以重试
   * 1.主题删除尚未完成
   * 2.该主题目前没有进行主题删除
   * 3.主题目前被标记为不适合删除
   * @param topic Topic
   * @return Whether or not deletion can be retried for the topic
   */
  private def isTopicEligibleForDeletion(topic: String): Boolean = {
    topicsToBeDeleted.contains(topic) && (!isTopicDeletionInProgress(topic) && !isTopicIneligibleForDeletion(topic))
  }

  /**
   * 如果该主题排队等待删除，但当前没有删除，则对该主题重试删除操作为确保重试成功，
    * 请将各个副本的状态从ReplicaDeletionIneligible重置为OfflineReplica状态
   *@param topic Topic for which deletion should be retried
   */
  private def markTopicForDeletionRetry(topic: String) {
    // reset replica states from ReplicaDeletionIneligible to OfflineReplica
    val failedReplicas = controller.replicaStateMachine.replicasInState(topic, ReplicaDeletionIneligible)
    info(s"Retrying delete topic for topic $topic since replicas ${failedReplicas.mkString(",")} were not successfully deleted")
    controller.replicaStateMachine.handleStateChanges(failedReplicas.toSeq, OfflineReplica)  // 标记副本下线
  }

  private def completeDeleteTopic(topic: String) {
    // 取消注册已删除主题上的分区更改侦听器。 这是为了防止当新主题侦听器在删除的主题被自动创建时触发分区更改侦听器
    controller.unregisterPartitionModificationsHandlers(Seq(topic))
    val replicasForDeletedTopic = controller.replicaStateMachine.replicasInState(topic, ReplicaDeletionSuccessful)
    // 控制器将从状态机及其分区分配缓存中删除该副本
    controller.replicaStateMachine.handleStateChanges(replicasForDeletedTopic.toSeq, NonExistentReplica)
    val partitionsForDeletedTopic = controllerContext.partitionsForTopic(topic)
    // 将相应的分区移至OfflinePartition和NonExistentPartition状态
    controller.partitionStateMachine.handleStateChanges(partitionsForDeletedTopic.toSeq, OfflinePartition)
    controller.partitionStateMachine.handleStateChanges(partitionsForDeletedTopic.toSeq, NonExistentPartition)
    // 移除所有关于主题的内容
    topicsToBeDeleted -= topic
    partitionsToBeDeleted.retain(_.topic != topic)
    zkClient.deleteTopicZNode(topic)
    zkClient.deleteTopicConfigs(Seq(topic))
    zkClient.deleteTopicDeletions(Seq(topic))
    controllerContext.removeTopic(topic)
  }

  /**
   * 用要删除的主题列表调用它为onPartitionDeletion调用主题的所有分区。
   * updateMetadataRequest也将把要删除的主题的领导者设置为{@link LeaderAndIsr＃LeaderDuringDelete}。
    * 这让每个经纪人都知道这个话题正在被删除，并且可以从他们的缓存中删除。
   */
  private def onTopicDeletion(topics: Set[String]) {
    info(s"Topic deletion callback for ${topics.mkString(",")}")
    // 发送更新元数据，以便经纪人停止投放要删除主题的数据
    val partitions = topics.flatMap(controllerContext.partitionsForTopic)
    controller.sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, partitions)
    val partitionReplicaAssignmentByTopic = controllerContext.partitionReplicaAssignment.groupBy(p => p._1.topic)
    topics.foreach { topic =>
      onPartitionDeletion(partitionReplicaAssignmentByTopic(topic).keySet)
    }
  }

  /**
   * 由onPartitionDeletion调用。这是删除主题的第二步，第一步是向所有代理发送UpdateMetadata请求，以开始拒绝已删除主题的请求。
    * 作为开始删除的一部分，这些主题将添加到正在进行的列表中。只要主题在进行中，就不会重试该主题的删除。当一个主题从进行中的列表中删除时
   * 1.主题被成功删除或
   * 2.主题的副本未处于ReplicaDeletionStarted状态，并且至少有一个副本处于ReplicaDeletionIneligible状态如果主题排队进行删除，
    * 但当前没有进行删除，则对该主题重试删除作为开始删除的一部分，所有副本移到ReplicaDeletionStarted状态，
    * 控制器将副本发送给StopReplicaRequest（delete = true）
   * 此方法执行以下操作 -
   * 1.将所有无效副本直接移至ReplicaDeletionIneligible状态。如果某些副本已经死了，也将标记为不适合删除的相应主题，
    * 因为无论如何都无法成功完成
   * 2.将所有活动副本移动到ReplicaDeletionStarted状态，以便可以成功删除它们
   *
   *
   *@param replicasForTopicsToBeDeleted
   */
  private def startReplicaDeletion(replicasForTopicsToBeDeleted: Set[PartitionAndReplica]) {
    replicasForTopicsToBeDeleted.groupBy(_.topic).keys.foreach { topic =>
      // 主题中存活的副本
      val aliveReplicasForTopic = controllerContext.allLiveReplicas().filter(p => p.topic == topic)
      // 主题中死亡的副本
      val deadReplicasForTopic = replicasForTopicsToBeDeleted -- aliveReplicasForTopic
      // 主题中成功回收的副本
      val successfullyDeletedReplicas = controller.replicaStateMachine.replicasInState(topic, ReplicaDeletionSuccessful)
      // 重复回收的副本
      val replicasForDeletionRetry = aliveReplicasForTopic -- successfullyDeletedReplicas
      // move dead replicas directly to failed state
      controller.replicaStateMachine.handleStateChanges(deadReplicasForTopic.toSeq, ReplicaDeletionIneligible)
      // send stop replica to all followers that are not in the OfflineReplica state so they stop sending fetch requests to the leader
      controller.replicaStateMachine.handleStateChanges(replicasForDeletionRetry.toSeq, OfflineReplica)
      debug(s"Deletion started for replicas ${replicasForDeletionRetry.mkString(",")}")
      controller.replicaStateMachine.handleStateChanges(replicasForDeletionRetry.toSeq, ReplicaDeletionStarted,
        new Callbacks(stopReplicaResponseCallback = (stopReplicaResponseObj, replicaId) =>
          eventManager.put(controller.TopicDeletionStopReplicaResponseReceived(stopReplicaResponseObj, replicaId))))
      if (deadReplicasForTopic.nonEmpty) {
        debug(s"Dead Replicas (${deadReplicasForTopic.mkString(",")}) found for topic $topic")
        markTopicIneligibleForDeletion(Set(topic))
      }
    }
  }

  /**
   * 由onTopicDeletion调用待删除主题的分区列表
   * 它执行以下操作 -
   * 1.将UpdateMetadataRequest发送给所有正在删除的分区的实时代理（不关闭）。
    * 代理开始拒绝使用UnknownTopicOrPartitionException的所有客户端请求
   * 2.将分区的所有副本移到OfflineReplica状态。
    * 这将发送StopReplicaRequest到副本和LeaderAndIsrRequest给收缩ISR的领导。
    * 当领导者副本本身移动到OfflineReplica状态时，它将跳过发送LeaderAndIsrRequest，因为领导者将被更新为-1
   * 3.将所有副本移到ReplicaDeletionStarted状态。
    * 这将使用deletePartition = true发送StopReplicaRequest。 并且将从相应分区的所有副本中删除所有持久数据
   */
  private def onPartitionDeletion(partitionsToBeDeleted: Set[TopicPartition]) {
    info(s"Partition deletion callback for ${partitionsToBeDeleted.mkString(",")}")
    val replicasPerPartition = controllerContext.replicasForPartition(partitionsToBeDeleted)
    startReplicaDeletion(replicasPerPartition)
  }

  // 恢复删除
  private def resumeDeletions(): Unit = {
    val topicsQueuedForDeletion = Set.empty[String] ++ topicsToBeDeleted

    if (topicsQueuedForDeletion.nonEmpty)
      info(s"Handling deletion for topics ${topicsQueuedForDeletion.mkString(",")}")

    topicsQueuedForDeletion.foreach { topic => // 进行删除处理
      // 如果所有副本都被标记为已成功删除，则会删除主题
      if (controller.replicaStateMachine.areAllReplicasForTopicDeleted(topic)) {
        // 从控制器缓存和zookeeper清除这个话题的所有状态
        completeDeleteTopic(topic)
        info(s"Deletion of topic $topic successfully completed")
      } else {
        if (controller.replicaStateMachine.isAtLeastOneReplicaInDeletionStartedState(topic)) { // 至少有一个分区开始删除
          // ignore since topic deletion is in progress
          val replicasInDeletionStartedState = controller.replicaStateMachine.replicasInState(topic, ReplicaDeletionStarted)
          val replicaIds = replicasInDeletionStartedState.map(_.replica)
          val partitions = replicasInDeletionStartedState.map(_.topicPartition)
          info(s"Deletion for replicas ${replicaIds.mkString(",")} for partition ${partitions.mkString(",")} of topic $topic in progress")
        } else {
          // 如果你来到这里，那么在TopicDeletionStarted中没有副本，并且所有副本都不在TopicDeletionSuccessful中。
          // 这意味着，无论是给定的主题还是未启动删除或至少有一个失败的副本（这意味着删除主题应重试）。
          if (controller.replicaStateMachine.isAnyReplicaInState(topic, ReplicaDeletionIneligible)) {
            // mark topic for deletion retry
            markTopicForDeletionRetry(topic) // 标记副本下线用于重新尝试删除
          }
        }
      }
      // 如果符合删除条件，请尝试删除主题。
      if (isTopicEligibleForDeletion(topic)) {
        info(s"Deletion of topic $topic (re)started")
        // topic deletion will be kicked off
        onTopicDeletion(Set(topic))
      } else if (isTopicIneligibleForDeletion(topic)) {
        info(s"Not retrying deletion of topic $topic at this time since it is marked ineligible for deletion")
      }
    }
  }
}
