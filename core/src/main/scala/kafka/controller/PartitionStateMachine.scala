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

import kafka.api.LeaderAndIsr
import kafka.common.StateChangeFailedException
import kafka.server.KafkaConfig
import kafka.utils.Logging
import kafka.zk.{KafkaZkClient, TopicPartitionStateZNode}
import kafka.zk.KafkaZkClient.UpdateLeaderAndIsrResult
import org.apache.kafka.common.TopicPartition
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.Code

import scala.collection.mutable


/**
 * 这个类表示分区的状态机。 它定义了分区可以处于的状态，并且
 * 转换将分区移至另一合法状态。 分区可以处于的不同状态是 -
 * 1. NonExistentPartition：此状态表示该分区是从未创建的，或者已创建并删除。 有效的先前状态（如果存在）是OfflinePartition
 * 2. NewPartition：创建后，分区处于NewPartition状态。 在这种状态下，分区应该有分配给它的副本，但是还没有leader / isr。 以前的有效状态是NonExistentPartition
 * 3.在线分区：一旦领导者被选为分区，它就处于OnlinePartition状态。 以前的有效状态是NewPartition / OfflinePartition
 * 4. OfflinePartition：如果在成功选举领导者后，分区负责人死亡，则分区将移至OfflinePartition状态。 以前的有效状态是NewPartition / OnlinePartition
 *
 */
class PartitionStateMachine(config: KafkaConfig,
                            stateChangeLogger: StateChangeLogger,
                            controllerContext: ControllerContext,
                            topicDeletionManager: TopicDeletionManager,
                            zkClient: KafkaZkClient,
                            partitionState: mutable.Map[TopicPartition, PartitionState],
                            controllerBrokerRequestBatch: ControllerBrokerRequestBatch) extends Logging {
  private val controllerId = config.brokerId

  this.logIdent = s"[PartitionStateMachine controllerId=$controllerId] "

  /**
   * 在成功的控制器选举中被调用。
   */
  def startup() {
    info("Initializing partition state")
    initializePartitionState()
    info("Triggering online partition state changes")
    triggerOnlinePartitionStateChange()
    info(s"Started partition state machine with initial state -> $partitionState")
  }

  /**
   * Invoked on controller shutdown.
   */
  def shutdown() {
    partitionState.clear()
    info("Stopped partition state machine")
  }

  /**
   * 在启动分区状态机时调用，以设置zookeeper中所有现有分区的初始状态
   */
  private def initializePartitionState() {
    for (topicPartition <- controllerContext.partitionReplicaAssignment.keys) {
      // 检查分区是否存在leader和isr路径。 如果不是，那么它处于NEW状态
      controllerContext.partitionLeadershipInfo.get(topicPartition) match {
        case Some(currentLeaderIsrAndEpoch) =>
          //否则，检查分区的领导是否活着。 如果是，则处于联机状态，否则处于脱机状态
          if (controllerContext.isReplicaOnline(currentLeaderIsrAndEpoch.leaderAndIsr.leader, topicPartition))
          // 领导者还活着
            partitionState.put(topicPartition, OnlinePartition)
          else
            partitionState.put(topicPartition, OfflinePartition)
        case None =>
          partitionState.put(topicPartition, NewPartition)
      }
    }
  }

  /**
    * 此API调用NewPartition或OfflinePartition状态下的所有分区上的OnlinePartition状态更改。 这被称为成功的控制权选举和经纪人更改
   */
  def triggerOnlinePartitionStateChange() {
    // 尝试将NewPartition或OfflinePartition状态下的所有分区移动到OnlinePartition状态，但属于要删除主题的分区除外
    val partitionsToTrigger = partitionState.filter { case (partition, partitionState) =>
      !topicDeletionManager.isTopicQueuedUpForDeletion(partition.topic) &&
        (partitionState.equals(OfflinePartition) || partitionState.equals(NewPartition))
    }.keys.toSeq
    handleStateChanges(partitionsToTrigger, OnlinePartition, Option(OfflinePartitionLeaderElectionStrategy))
    // TODO: If handleStateChanges catches an exception, it is not enough to bail out and log an error.
    // It is important to trigger leader election for those partitions.
  }

  // 处理状态改变
  def handleStateChanges(partitions: Seq[TopicPartition], targetState: PartitionState,
                         partitionLeaderElectionStrategyOpt: Option[PartitionLeaderElectionStrategy] = None): Unit = {
    if (partitions.nonEmpty) {
      try {
        controllerBrokerRequestBatch.newBatch()
        doHandleStateChanges(partitions, targetState, partitionLeaderElectionStrategyOpt)
        controllerBrokerRequestBatch.sendRequestsToBrokers(controllerContext.epoch)
      } catch {
        case e: Throwable => error(s"Error while moving some partitions to $targetState state", e)
      }
    }
  }

  def partitionsInState(state: PartitionState): Set[TopicPartition] = {
    partitionState.filter { case (_, s) => s == state }.keySet.toSet
  }

  /**
   * 这个API执行分区的状态机。它确保每个状态转换都从一个合法的先前状态发生到目标状态。有效的状态转换是：
   * NonExistentPartition - > NewPartition：
   * - 将分配的副本从ZK加载到控制器缓存
   * NewPartition - > OnlinePartition
   *
   * - 分配第一个现场副本作为领导者和所有现场副本作为isr;写这个分区的领导和ISR到ZK
   * - 发送LeaderAndIsr请求给每个活动副本和UpdateMetadata请求给每个在线经纪人
   * OnlinePartition，OfflinePartition - > OnlinePartition
   *
   * - 选择新的领导者和ISR为这个分区和一组复制品接收LeaderAndIsr请求，并将领导者和ISR写入ZK
   * - 对于此分区，将LeaderAndIsr请求发送给每个接收副本和UpdateMetadata请求给每个实时代理
   * NewPartition，OnlinePartition，OfflinePartition - > OfflinePartition
   *
   * - 除了将分区状态标记为脱机以外
   * OfflinePartition - > NonExistentPartition
   *
   * - 除了将分区状态标记为NonExistentPartition外
   * @param partitions  The partitions for which the state transition is invoked
   * @param targetState The end state that the partition should be moved to
   */
  private def doHandleStateChanges(partitions: Seq[TopicPartition], targetState: PartitionState,
                           partitionLeaderElectionStrategyOpt: Option[PartitionLeaderElectionStrategy]): Unit = {
    val stateChangeLog = stateChangeLogger.withControllerEpoch(controllerContext.epoch)
    partitions.foreach(partition => partitionState.getOrElseUpdate(partition, NonExistentPartition)) // 将不存在的tp更新为NonExistentPartition
    val (validPartitions, invalidPartitions) = partitions.partition(partition => isValidTransition(partition, targetState))
    invalidPartitions.foreach(partition => logInvalidTransition(partition, targetState)) // 记录失败的日志
    targetState match {
      case NewPartition =>
        validPartitions.foreach { partition =>
          stateChangeLog.trace(s"Changed partition $partition state from ${partitionState(partition)} to $targetState with " +
            s"assigned replicas ${controllerContext.partitionReplicaAssignment(partition).mkString(",")}")
          partitionState.put(partition, NewPartition)
        }
      case OnlinePartition =>
        val uninitializedPartitions = validPartitions.filter(partition => partitionState(partition) == NewPartition) // 未初始化的分区
        // 用于选举领导者的分区
        val partitionsToElectLeader = validPartitions.filter(partition => partitionState(partition) == OfflinePartition || partitionState(partition) == OnlinePartition)
        if (uninitializedPartitions.nonEmpty) { // 初始化的处理
          val successfulInitializations = initializeLeaderAndIsrForPartitions(uninitializedPartitions)
          successfulInitializations.foreach { partition =>
            stateChangeLog.trace(s"Changed partition $partition from ${partitionState(partition)} to $targetState with state " +
              s"${controllerContext.partitionLeadershipInfo(partition).leaderAndIsr}")
            partitionState.put(partition, OnlinePartition)
          }
        }
        if (partitionsToElectLeader.nonEmpty) { // 重新选举领导者的选举
          val successfulElections = electLeaderForPartitions(partitionsToElectLeader, partitionLeaderElectionStrategyOpt.get)
          successfulElections.foreach { partition =>
            stateChangeLog.trace(s"Changed partition $partition from ${partitionState(partition)} to $targetState with state " +
              s"${controllerContext.partitionLeadershipInfo(partition).leaderAndIsr}")
            partitionState.put(partition, OnlinePartition)
          }
        }
      case OfflinePartition =>
        validPartitions.foreach { partition =>
          stateChangeLog.trace(s"Changed partition $partition state from ${partitionState(partition)} to $targetState")
          partitionState.put(partition, OfflinePartition)
        }
      case NonExistentPartition =>
        validPartitions.foreach { partition =>
          stateChangeLog.trace(s"Changed partition $partition state from ${partitionState(partition)} to $targetState")
          partitionState.put(partition, NonExistentPartition)
        }
    }
  }

  /**
   * 在zookeeper中初始化leader和isr分区状态。
   * @param partitions The partitions  that we're trying to initialize.
   * @return The partitions that have been successfully initialized.
   */
  private def initializeLeaderAndIsrForPartitions(partitions: Seq[TopicPartition]): Seq[TopicPartition] = {
    val successfulInitializations = mutable.Buffer.empty[TopicPartition]
    // 每个分区的备份
    val replicasPerPartition = partitions.map(partition => partition -> controllerContext.partitionReplicaAssignment(partition))
    // 每个分区存活的备份
    val liveReplicasPerPartition = replicasPerPartition.map { case (partition, replicas) =>
        val liveReplicasForPartition = replicas.filter(replica => controllerContext.isReplicaOnline(replica, partition))
        partition -> liveReplicasForPartition
    }
    val (partitionsWithoutLiveReplicas, partitionsWithLiveReplicas) = liveReplicasPerPartition.partition { case (_, liveReplicas) => liveReplicas.isEmpty }
    // 没有存活备份分区需要日志打印
    partitionsWithoutLiveReplicas.foreach { case (partition, replicas) =>
      val failMsg = s"Controller $controllerId epoch ${controllerContext.epoch} encountered error during state change of " +
        s"partition $partition from New to Online, assigned replicas are " +
        s"[${replicas.mkString(",")}], live brokers are [${controllerContext.liveBrokerIds}]. No assigned " +
        "replica is alive."
      logFailedStateChange(partition, NewPartition, OnlinePartition, new StateChangeFailedException(failMsg))
    }
    val leaderIsrAndControllerEpochs = partitionsWithLiveReplicas.map { case (partition, liveReplicas) =>
      val leaderAndIsr = LeaderAndIsr(liveReplicas.head, liveReplicas.toList)
      val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerContext.epoch)
      partition -> leaderIsrAndControllerEpoch
    }.toMap
    val createResponses = try {
      zkClient.createTopicPartitionStatesRaw(leaderIsrAndControllerEpochs)
    } catch {
      case e: Exception =>
        partitionsWithLiveReplicas.foreach { case (partition,_) => logFailedStateChange(partition, partitionState(partition), NewPartition, e) }
        Seq.empty
    }
    createResponses.foreach { createResponse =>
      val code = createResponse.resultCode
      val partition = createResponse.ctx.get.asInstanceOf[TopicPartition]
      val leaderIsrAndControllerEpoch = leaderIsrAndControllerEpochs(partition)
      if (code == Code.OK) {
        controllerContext.partitionLeadershipInfo.put(partition, leaderIsrAndControllerEpoch)
        // 发送ISR请求
        controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(leaderIsrAndControllerEpoch.leaderAndIsr.isr,
          partition, leaderIsrAndControllerEpoch, controllerContext.partitionReplicaAssignment(partition), isNew = true)
        successfulInitializations += partition
      } else {
        logFailedStateChange(partition, NewPartition, OnlinePartition, code)
      }
    }
    successfulInitializations
  }

  /**
   * 重复尝试选择多个分区的领导者，直到没有剩余的分区重试为止。
   * @param partitions The partitions that we're trying to elect leaders for.
   * @param partitionLeaderElectionStrategy The election strategy to use.
   * @return The partitions that successfully had a leader elected.
   */
  private def electLeaderForPartitions(partitions: Seq[TopicPartition], partitionLeaderElectionStrategy: PartitionLeaderElectionStrategy): Seq[TopicPartition] = {
    val successfulElections = mutable.Buffer.empty[TopicPartition]
    var remaining = partitions
    while (remaining.nonEmpty) {
      val (success, updatesToRetry, failedElections) = doElectLeaderForPartitions(partitions, partitionLeaderElectionStrategy)
      remaining = updatesToRetry  // 需要重试的
      successfulElections ++= success  // 成功的
      failedElections.foreach { case (partition, e) => // 失败的
        logFailedStateChange(partition, partitionState(partition), OnlinePartition, e)
      }
    }
    successfulElections
  }

  /**
   * 尝试选择多个分区的领导者。 选择分区的领导者更新zookeeper中的分区状态。
   *
   * @param partitions The partitions that we're trying to elect leaders for.
   * @param partitionLeaderElectionStrategy The election strategy to use.
   * @return A tuple of three values:
   *         1. The partitions that successfully had a leader elected.
   *         2. The partitions that we should retry due to a zookeeper BADVERSION conflict. Version conflicts can occur if
   *         the partition leader updated partition state while the controller attempted to update partition state.
   *         3. Exceptions corresponding to failed elections that should not be retried.
   */
  private def doElectLeaderForPartitions(partitions: Seq[TopicPartition], partitionLeaderElectionStrategy: PartitionLeaderElectionStrategy):
  (Seq[TopicPartition], Seq[TopicPartition], Map[TopicPartition, Exception]) = {
    val getDataResponses = try {
      zkClient.getTopicPartitionStatesRaw(partitions)
    } catch {
      case e: Exception =>
        return (Seq.empty, Seq.empty, partitions.map(_ -> e).toMap)
    }
    val failedElections = mutable.Map.empty[TopicPartition, Exception]
    // 从zk获取的ISR
    val leaderIsrAndControllerEpochPerPartition = mutable.Buffer.empty[(TopicPartition, LeaderIsrAndControllerEpoch)]
    getDataResponses.foreach { getDataResponse =>
      val partition = getDataResponse.ctx.get.asInstanceOf[TopicPartition]
      val currState = partitionState(partition)
      if (getDataResponse.resultCode == Code.OK) {
        val leaderIsrAndControllerEpochOpt = TopicPartitionStateZNode.decode(getDataResponse.data, getDataResponse.stat)
        if (leaderIsrAndControllerEpochOpt.isEmpty) {
          val exception = new StateChangeFailedException(s"LeaderAndIsr information doesn't exist for partition $partition in $currState state")
          failedElections.put(partition, exception)
        }
        leaderIsrAndControllerEpochPerPartition += partition -> leaderIsrAndControllerEpochOpt.get
      } else if (getDataResponse.resultCode == Code.NONODE) {
        val exception = new StateChangeFailedException(s"LeaderAndIsr information doesn't exist for partition $partition in $currState state")
        failedElections.put(partition, exception)
      } else {
        failedElections.put(partition, getDataResponse.resultException.get)
      }
    }
    val (invalidPartitionsForElection, validPartitionsForElection) = leaderIsrAndControllerEpochPerPartition.partition { case (partition, leaderIsrAndControllerEpoch) =>
      leaderIsrAndControllerEpoch.controllerEpoch > controllerContext.epoch
    }
    // 无效的分区，即大于当前控制器的epoch
    invalidPartitionsForElection.foreach { case (partition, leaderIsrAndControllerEpoch) =>
      val failMsg = s"aborted leader election for partition $partition since the LeaderAndIsr path was " +
        s"already written by another controller. This probably means that the current controller $controllerId went through " +
        s"a soft failure and another controller was elected with epoch ${leaderIsrAndControllerEpoch.controllerEpoch}."
      failedElections.put(partition, new StateChangeFailedException(failMsg))
    }
    if (validPartitionsForElection.isEmpty) {
      return (Seq.empty, Seq.empty, failedElections.toMap)
    }
    val shuttingDownBrokers  = controllerContext.shuttingDownBrokerIds.toSet
    val (partitionsWithoutLeaders, partitionsWithLeaders) = partitionLeaderElectionStrategy match {
      case OfflinePartitionLeaderElectionStrategy =>
        leaderForOffline(validPartitionsForElection).partition { case (_, newLeaderAndIsrOpt, _) => newLeaderAndIsrOpt.isEmpty }
      case ReassignPartitionLeaderElectionStrategy =>
        leaderForReassign(validPartitionsForElection).partition { case (_, newLeaderAndIsrOpt, _) => newLeaderAndIsrOpt.isEmpty }
      case PreferredReplicaPartitionLeaderElectionStrategy =>
        leaderForPreferredReplica(validPartitionsForElection).partition { case (_, newLeaderAndIsrOpt, _) => newLeaderAndIsrOpt.isEmpty }
      case ControlledShutdownPartitionLeaderElectionStrategy =>
        leaderForControlledShutdown(validPartitionsForElection, shuttingDownBrokers).partition { case (_, newLeaderAndIsrOpt, _) => newLeaderAndIsrOpt.isEmpty }
    }
    // 没有产生领导者分区
    partitionsWithoutLeaders.foreach { case (partition, leaderAndIsrOpt, recipients) =>
      val failMsg = s"Failed to elect leader for partition $partition under strategy $partitionLeaderElectionStrategy"
      failedElections.put(partition, new StateChangeFailedException(failMsg))
    }
    val recipientsPerPartition = partitionsWithLeaders.map { case (partition, leaderAndIsrOpt, recipients) => partition -> recipients }.toMap
    val adjustedLeaderAndIsrs = partitionsWithLeaders.map { case (partition, leaderAndIsrOpt, recipients) => partition -> leaderAndIsrOpt.get }.toMap
    val UpdateLeaderAndIsrResult(successfulUpdates, updatesToRetry, failedUpdates) = zkClient.updateLeaderAndIsr(  // 像zk更新ISR
      adjustedLeaderAndIsrs, controllerContext.epoch)
    successfulUpdates.foreach { case (partition, leaderAndIsr) =>
      val replicas = controllerContext.partitionReplicaAssignment(partition)
      val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerContext.epoch)
      controllerContext.partitionLeadershipInfo.put(partition, leaderIsrAndControllerEpoch)  // 更新上下文领导者信息
      controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(recipientsPerPartition(partition), partition,  // 通知其他broker
        leaderIsrAndControllerEpoch, replicas, isNew = false)
    }
    (successfulUpdates.keys.toSeq, updatesToRetry, failedElections.toMap ++ failedUpdates)
  }

  private def leaderForOffline(leaderIsrAndControllerEpochs: Seq[(TopicPartition, LeaderIsrAndControllerEpoch)]):
  Seq[(TopicPartition, Option[LeaderAndIsr], Seq[Int])] = {
    val (partitionsWithNoLiveInSyncReplicas, partitionsWithLiveInSyncReplicas) = leaderIsrAndControllerEpochs.partition { case (partition, leaderIsrAndControllerEpoch) =>
      val liveInSyncReplicas = leaderIsrAndControllerEpoch.leaderAndIsr.isr.filter(replica => controllerContext.isReplicaOnline(replica, partition))
      liveInSyncReplicas.isEmpty
    }
    // 没有存活的副本
    val (logConfigs, failed) = zkClient.getLogConfigs(partitionsWithNoLiveInSyncReplicas.map { case (partition, _) => partition.topic }, config.originals())
    val partitionsWithUncleanLeaderElectionState = partitionsWithNoLiveInSyncReplicas.map { case (partition, leaderIsrAndControllerEpoch) =>
      if (failed.contains(partition.topic)) {
        logFailedStateChange(partition, partitionState(partition), OnlinePartition, failed(partition.topic))
        (partition, None, false)
      } else {
        (partition, Option(leaderIsrAndControllerEpoch), logConfigs(partition.topic).uncleanLeaderElectionEnable.booleanValue())
      }
    } ++ partitionsWithLiveInSyncReplicas.map { case (partition, leaderIsrAndControllerEpoch) => (partition, Option(leaderIsrAndControllerEpoch), false) }

    // 带有清除状态的分区状态
    partitionsWithUncleanLeaderElectionState.map { case (partition, leaderIsrAndControllerEpochOpt, uncleanLeaderElectionEnabled) =>
      val assignment = controllerContext.partitionReplicaAssignment(partition) // 已经分配的副本
      val liveReplicas = assignment.filter(replica => controllerContext.isReplicaOnline(replica, partition))
      if (leaderIsrAndControllerEpochOpt.nonEmpty) {
        val leaderIsrAndControllerEpoch = leaderIsrAndControllerEpochOpt.get
        val isr = leaderIsrAndControllerEpoch.leaderAndIsr.isr
        // 计算新的新的领导者
        val leaderOpt = PartitionLeaderElectionAlgorithms.offlinePartitionLeaderElection(assignment, isr, liveReplicas.toSet, uncleanLeaderElectionEnabled)
        // 产生新的leader和ISR
        val newLeaderAndIsrOpt = leaderOpt.map { leader =>
          val newIsr = if (isr.contains(leader)) isr.filter(replica => controllerContext.isReplicaOnline(replica, partition))
          else List(leader)
          leaderIsrAndControllerEpoch.leaderAndIsr.newLeaderAndIsr(leader, newIsr)
        }
        (partition, newLeaderAndIsrOpt, liveReplicas)
      } else {
        (partition, None, liveReplicas)
      }
    }
  }

  private def leaderForReassign(leaderIsrAndControllerEpochs: Seq[(TopicPartition, LeaderIsrAndControllerEpoch)]):
  Seq[(TopicPartition, Option[LeaderAndIsr], Seq[Int])] = {
    leaderIsrAndControllerEpochs.map { case (partition, leaderIsrAndControllerEpoch) =>
      val reassignment = controllerContext.partitionsBeingReassigned(partition).newReplicas
      val liveReplicas = reassignment.filter(replica => controllerContext.isReplicaOnline(replica, partition))
      val isr = leaderIsrAndControllerEpoch.leaderAndIsr.isr
      val leaderOpt = PartitionLeaderElectionAlgorithms.reassignPartitionLeaderElection(reassignment, isr, liveReplicas.toSet)
      val newLeaderAndIsrOpt = leaderOpt.map(leader => leaderIsrAndControllerEpoch.leaderAndIsr.newLeader(leader))
      (partition, newLeaderAndIsrOpt, reassignment)
    }
  }

  private def leaderForPreferredReplica(leaderIsrAndControllerEpochs: Seq[(TopicPartition, LeaderIsrAndControllerEpoch)]):
  Seq[(TopicPartition, Option[LeaderAndIsr], Seq[Int])] = {
    leaderIsrAndControllerEpochs.map { case (partition, leaderIsrAndControllerEpoch) =>
      val assignment = controllerContext.partitionReplicaAssignment(partition)
      val liveReplicas = assignment.filter(replica => controllerContext.isReplicaOnline(replica, partition))
      val isr = leaderIsrAndControllerEpoch.leaderAndIsr.isr
      val leaderOpt = PartitionLeaderElectionAlgorithms.preferredReplicaPartitionLeaderElection(assignment, isr, liveReplicas.toSet)
      val newLeaderAndIsrOpt = leaderOpt.map(leader => leaderIsrAndControllerEpoch.leaderAndIsr.newLeader(leader))
      (partition, newLeaderAndIsrOpt, assignment)
    }
  }

  private def leaderForControlledShutdown(leaderIsrAndControllerEpochs: Seq[(TopicPartition, LeaderIsrAndControllerEpoch)], shuttingDownBrokers: Set[Int]):
  Seq[(TopicPartition, Option[LeaderAndIsr], Seq[Int])] = {
    leaderIsrAndControllerEpochs.map { case (partition, leaderIsrAndControllerEpoch) =>
      val assignment = controllerContext.partitionReplicaAssignment(partition)
      val liveReplicas = assignment.filter(replica => controllerContext.isReplicaOnline(replica, partition))
      val isr = leaderIsrAndControllerEpoch.leaderAndIsr.isr
      val leaderOpt = PartitionLeaderElectionAlgorithms.controlledShutdownPartitionLeaderElection(assignment, isr, liveReplicas.toSet, shuttingDownBrokers)
      val newIsr = isr.filter(replica => !controllerContext.shuttingDownBrokerIds.contains(replica))
      val newLeaderAndIsrOpt = leaderOpt.map(leader => leaderIsrAndControllerEpoch.leaderAndIsr.newLeaderAndIsr(leader, newIsr))
      (partition, newLeaderAndIsrOpt, liveReplicas)
    }
  }

  // 是否是有效的状态转换事务
  private def isValidTransition(partition: TopicPartition, targetState: PartitionState) =
    targetState.validPreviousStates.contains(partitionState(partition))

  // 记录无效的事务
  private def logInvalidTransition(partition: TopicPartition, targetState: PartitionState): Unit = {
    val currState = partitionState(partition)
    val e = new IllegalStateException(s"Partition $partition should be in one of " +
      s"${targetState.validPreviousStates.mkString(",")} states before moving to $targetState state. Instead it is in " +
      s"$currState state")
    logFailedStateChange(partition, currState, targetState, e)
  }

  private def logFailedStateChange(partition: TopicPartition, currState: PartitionState, targetState: PartitionState, code: Code): Unit = {
    logFailedStateChange(partition, currState, targetState, KeeperException.create(code))
  }

  // 记录失败日志
  private def logFailedStateChange(partition: TopicPartition, currState: PartitionState, targetState: PartitionState, t: Throwable): Unit = {
    stateChangeLogger.withControllerEpoch(controllerContext.epoch)
      .error(s"Controller $controllerId epoch ${controllerContext.epoch} failed to change state for partition $partition " +
        s"from $currState to $targetState", t)
  }
}

// 分区选举领导者算法
object PartitionLeaderElectionAlgorithms {
  def offlinePartitionLeaderElection(assignment: Seq[Int], isr: Seq[Int], liveReplicas: Set[Int], uncleanLeaderElectionEnabled: Boolean): Option[Int] = {
    assignment.find(id => liveReplicas.contains(id) && isr.contains(id)).orElse {
      if (uncleanLeaderElectionEnabled) {
        assignment.find(liveReplicas.contains)
      } else {
        None
      }
    }
  }

  def reassignPartitionLeaderElection(reassignment: Seq[Int], isr: Seq[Int], liveReplicas: Set[Int]): Option[Int] = {
    reassignment.find(id => liveReplicas.contains(id) && isr.contains(id))
  }

  def preferredReplicaPartitionLeaderElection(assignment: Seq[Int], isr: Seq[Int], liveReplicas: Set[Int]): Option[Int] = {
    assignment.headOption.filter(id => liveReplicas.contains(id) && isr.contains(id))
  }

  def controlledShutdownPartitionLeaderElection(assignment: Seq[Int], isr: Seq[Int], liveReplicas: Set[Int], shuttingDownBrokers: Set[Int]): Option[Int] = {
    assignment.find(id => liveReplicas.contains(id) && isr.contains(id) && !shuttingDownBrokers.contains(id))
  }
}

// 分区领导者选举策略
sealed trait PartitionLeaderElectionStrategy
case object OfflinePartitionLeaderElectionStrategy extends PartitionLeaderElectionStrategy
case object ReassignPartitionLeaderElectionStrategy extends PartitionLeaderElectionStrategy
case object PreferredReplicaPartitionLeaderElectionStrategy extends PartitionLeaderElectionStrategy
case object ControlledShutdownPartitionLeaderElectionStrategy extends PartitionLeaderElectionStrategy

// 分区状态
sealed trait PartitionState {
  def state: Byte
  def validPreviousStates: Set[PartitionState]  // 有效的前缀状态
}

case object NewPartition extends PartitionState {
  val state: Byte = 0
  val validPreviousStates: Set[PartitionState] = Set(NonExistentPartition)
}

case object OnlinePartition extends PartitionState {
  val state: Byte = 1
  val validPreviousStates: Set[PartitionState] = Set(NewPartition, OnlinePartition, OfflinePartition)
}

case object OfflinePartition extends PartitionState {
  val state: Byte = 2
  val validPreviousStates: Set[PartitionState] = Set(NewPartition, OnlinePartition, OfflinePartition)
}

case object NonExistentPartition extends PartitionState {
  val state: Byte = 3
  val validPreviousStates: Set[PartitionState] = Set(OfflinePartition)
}
