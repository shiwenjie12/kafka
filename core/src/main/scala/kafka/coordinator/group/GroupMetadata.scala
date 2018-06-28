/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.coordinator.group

import java.util.UUID
import java.util.concurrent.locks.ReentrantLock

import kafka.common.OffsetAndMetadata
import kafka.utils.{CoreUtils, Logging, nonthreadsafe}
import org.apache.kafka.common.TopicPartition

import scala.collection.{Seq, immutable, mutable}

private[group] sealed trait GroupState

/**
 * group正在准备重新平衡
 *
 * action: respond to heartbeats with REBALANCE_IN_PROGRESS
 *         respond to sync group with REBALANCE_IN_PROGRESS
 *         remove member on leave group request
 *         park join group requests from new or existing members until all expected members have joined
 *         allow offset commits from previous generation
 *         allow offset fetch requests
 * transition: some members have joined by the timeout => CompletingRebalance
 *             all members have left the group => Empty
 *             group is removed by partition emigration => Dead
 */
private[group] case object PreparingRebalance extends GroupState

/**
 * group正在等待领导者的状态分配
 *
 * action: respond to heartbeats with REBALANCE_IN_PROGRESS
 *         respond to offset commits with REBALANCE_IN_PROGRESS
 *         park sync group requests from followers until transition to Stable
 *         allow offset fetch requests
 * transition: sync group with state assignment received from leader => Stable
 *             join group from new member or existing member with updated metadata => PreparingRebalance
 *             leave group from existing member => PreparingRebalance
 *             member failure detected => PreparingRebalance
 *             group is removed by partition emigration => Dead
 */
private[group] case object CompletingRebalance extends GroupState

/**
 * Group是稳定的
 *
 * action: respond to member heartbeats normally
 *         respond to sync group from any member with current assignment
 *         respond to join group from followers with matching metadata with current group metadata
 *         allow offset commits from member of current generation
 *         allow offset fetch requests
 * transition: member failure detected via heartbeat => PreparingRebalance
 *             leave group from existing member => PreparingRebalance
 *             leader join-group received => PreparingRebalance
 *             follower join-group with new metadata => PreparingRebalance
 *             group is removed by partition emigration => Dead
 */
private[group] case object Stable extends GroupState

/**
 * group没有更多成员，其元数据正在被删除
 *
 * action: respond to join group with UNKNOWN_MEMBER_ID
 *         respond to sync group with UNKNOWN_MEMBER_ID
 *         respond to heartbeat with UNKNOWN_MEMBER_ID
 *         respond to leave group with UNKNOWN_MEMBER_ID
 *         respond to offset commit with UNKNOWN_MEMBER_ID
 *         allow offset fetch requests
 * transition: Dead is a final state before group metadata is cleaned up, so there are no transitions
 */
private[group] case object Dead extends GroupState

/**
  * 小组没有更多成员，但是直到所有偏移都已经过期。 这种状态也代表使用卡夫卡仅用于抵消提交并且没有成员的组。...
  *
  * action: respond normally to join group from new members
  *         respond to sync group with UNKNOWN_MEMBER_ID
  *         respond to heartbeat with UNKNOWN_MEMBER_ID
  *         respond to leave group with UNKNOWN_MEMBER_ID
  *         respond to offset commit with UNKNOWN_MEMBER_ID
  *         allow offset fetch requests
  * transition: last offsets removed in periodic expiration task => Dead
  *             join group from a new member => PreparingRebalance
  *             group is removed by partition emigration => Dead
  *             group is removed by expiration => Dead
  */
private[group] case object Empty extends GroupState


private object GroupMetadata {
  // 有效的前置状态
  private val validPreviousStates: Map[GroupState, Set[GroupState]] =
    Map(Dead -> Set(Stable, PreparingRebalance, CompletingRebalance, Empty, Dead),
      CompletingRebalance -> Set(PreparingRebalance),
      Stable -> Set(CompletingRebalance),
      PreparingRebalance -> Set(Stable, CompletingRebalance, Empty),
      Empty -> Set(PreparingRebalance))

  def loadGroup(groupId: String,
                initialState: GroupState,
                generationId: Int,
                protocolType: String,
                protocol: String,
                leaderId: String,
                members: Iterable[MemberMetadata]): GroupMetadata = {
    val group = new GroupMetadata(groupId, initialState)
    group.generationId = generationId
    group.protocolType = if (protocolType == null || protocolType.isEmpty) None else Some(protocolType)
    group.protocol = Option(protocol)
    group.leaderId = Option(leaderId)
    members.foreach(group.add)
    group
  }
}

/**
 * Case class used to represent group metadata for the ListGroups API
 */
case class GroupOverview(groupId: String,
                         protocolType: String)

/**
 * Case class used to represent group metadata for the DescribeGroup API
 */
case class GroupSummary(state: String,
                        protocolType: String,
                        protocol: String,
                        members: List[MemberSummary])

/**
  * 我们缓存偏移提交及其提交记录偏移量。
  * 这使我们能够确保在我们混合使用事务性和常规偏移量提交时始终实现最新的偏移量提交。
  * 在不保留提交记录偏移的信息的情况下，压缩偏移主题它可能导致错误的偏移提交被实现。
  */
case class CommitRecordMetadataAndOffset(appendedBatchOffset: Option[Long], offsetAndMetadata: OffsetAndMetadata) {
  def olderThan(that: CommitRecordMetadataAndOffset) : Boolean = appendedBatchOffset.get < that.appendedBatchOffset.get
}

/**
 * 组包含以下元数据：
 *
 *   Membership metadata:
 *   1.在此组中注册的会员
 *   2.分配给组的当前协议（例如消费者的分区分配策略）
 *   3.与组成员相关的协议元数据
 *
 *   State metadata:
 *   1.团体状态
 *   2.生成ID
 *   3.领导者身份证
 */
@nonthreadsafe
private[group] class GroupMetadata(val groupId: String, initialState: GroupState) extends Logging {
  private[group] val lock = new ReentrantLock

  // 分组状态
  private var state: GroupState = initialState
  var protocolType: Option[String] = None
  // 构造出的唯一标识符
  var generationId = 0
  private var leaderId: Option[String] = None
  private var protocol: Option[String] = None

  private val members = new mutable.HashMap[String, MemberMetadata]
  private val offsets = new mutable.HashMap[TopicPartition, CommitRecordMetadataAndOffset]
  // 等待提交的tp偏移量
  private val pendingOffsetCommits = new mutable.HashMap[TopicPartition, OffsetAndMetadata]
  private val pendingTransactionalOffsetCommits = new mutable.HashMap[Long, mutable.Map[TopicPartition, CommitRecordMetadataAndOffset]]()
  private var receivedTransactionalOffsetCommits = false
  private var receivedConsumerOffsetCommits = false

  var newMemberAdded: Boolean = false

  def inLock[T](fun: => T): T = CoreUtils.inLock(lock)(fun)

  def is(groupState: GroupState) = state == groupState
  def not(groupState: GroupState) = state != groupState
  def has(memberId: String) = members.contains(memberId)
  def get(memberId: String) = members(memberId)

  def isLeader(memberId: String): Boolean = leaderId.contains(memberId)
  def leaderOrNull: String = leaderId.orNull
  def protocolOrNull: String = protocol.orNull

  def add(member: MemberMetadata) {
    if (members.isEmpty)
      this.protocolType = Some(member.protocolType)

    assert(groupId == member.groupId)
    assert(this.protocolType.orNull == member.protocolType)
    assert(supportsProtocols(member.protocols))

    if (leaderId.isEmpty)
      leaderId = Some(member.memberId)
    members.put(member.memberId, member)
  }

  def remove(memberId: String) {
    members.remove(memberId)
    if (isLeader(memberId)) {
      leaderId = if (members.isEmpty) {
        None
      } else {
        Some(members.keys.head)
      }
    }
  }

  def currentState = state

  def notYetRejoinedMembers = members.values.filter(_.awaitingJoinCallback == null).toList

  def allMembers = members.keySet

  def allMemberMetadata = members.values.toList

  def rebalanceTimeoutMs = members.values.foldLeft(0) { (timeout, member) =>
    timeout.max(member.rebalanceTimeoutMs)
  }

  // TODO: decide if ids should be predictable or random
  def generateMemberIdSuffix = UUID.randomUUID().toString

  def canRebalance = GroupMetadata.validPreviousStates(PreparingRebalance).contains(state)

  // 设置分组状态
  def transitionTo(groupState: GroupState) {
    assertValidTransition(groupState)
    state = groupState
  }

  def selectProtocol: String = {
    if (members.isEmpty)
      throw new IllegalStateException("Cannot select protocol for empty group")

    // select the protocol for this group which is supported by all members
    val candidates = candidateProtocols

    // let each member vote for one of the protocols and choose the one with the most votes
    val votes: List[(String, Int)] = allMemberMetadata
      .map(_.vote(candidates))
      .groupBy(identity)
      .mapValues(_.size)
      .toList

    votes.maxBy(_._2)._1
  }

  private def candidateProtocols = {
    // get the set of protocols that are commonly supported by all members
    allMemberMetadata
      .map(_.protocols)
      .reduceLeft((commonProtocols, protocols) => commonProtocols & protocols)
  }

  def supportsProtocols(memberProtocols: Set[String]) = {
    members.isEmpty || (memberProtocols & candidateProtocols).nonEmpty
  }

  // 初始化下一次版本
  def initNextGeneration() = {
    assert(notYetRejoinedMembers == List.empty[MemberMetadata])
    if (members.nonEmpty) {
      generationId += 1
      protocol = Some(selectProtocol)
      transitionTo(CompletingRebalance)
    } else {
      generationId += 1
      protocol = None
      transitionTo(Empty)
    }
    receivedConsumerOffsetCommits = false
    receivedTransactionalOffsetCommits = false
  }

  def currentMemberMetadata: Map[String, Array[Byte]] = {
    if (is(Dead) || is(PreparingRebalance))
      throw new IllegalStateException("Cannot obtain member metadata for group in state %s".format(state))
    members.map{ case (memberId, memberMetadata) => (memberId, memberMetadata.metadata(protocol.get))}.toMap
  }

  def summary: GroupSummary = {
    if (is(Stable)) {
      val protocol = protocolOrNull
      if (protocol == null)
        throw new IllegalStateException("Invalid null group protocol for stable group")

      val members = this.members.values.map { member => member.summary(protocol) }
      GroupSummary(state.toString, protocolType.getOrElse(""), protocol, members.toList)
    } else {
      val members = this.members.values.map{ member => member.summaryNoMetadata() }
      GroupSummary(state.toString, protocolType.getOrElse(""), GroupCoordinator.NoProtocol, members.toList)
    }
  }

  def overview: GroupOverview = {
    GroupOverview(groupId, protocolType.getOrElse(""))
  }

  def initializeOffsets(offsets: collection.Map[TopicPartition, CommitRecordMetadataAndOffset],
                        pendingTxnOffsets: Map[Long, mutable.Map[TopicPartition, CommitRecordMetadataAndOffset]]) {
    this.offsets ++= offsets
    this.pendingTransactionalOffsetCommits ++= pendingTxnOffsets
  }

  def onOffsetCommitAppend(topicPartition: TopicPartition, offsetWithCommitRecordMetadata: CommitRecordMetadataAndOffset) {
    if (pendingOffsetCommits.contains(topicPartition)) {
      if (offsetWithCommitRecordMetadata.appendedBatchOffset.isEmpty)
        throw new IllegalStateException("Cannot complete offset commit write without providing the metadata of the record " +
          "in the log.")
      if (!offsets.contains(topicPartition) || offsets(topicPartition).olderThan(offsetWithCommitRecordMetadata))
        offsets.put(topicPartition, offsetWithCommitRecordMetadata)
    }

    pendingOffsetCommits.get(topicPartition) match {
      case Some(stagedOffset) if offsetWithCommitRecordMetadata.offsetAndMetadata == stagedOffset =>
        pendingOffsetCommits.remove(topicPartition)
      case _ =>
        // The pendingOffsetCommits for this partition could be empty if the topic was deleted, in which case
        // its entries would be removed from the cache by the `removeOffsets` method.
    }
  }

  def failPendingOffsetWrite(topicPartition: TopicPartition, offset: OffsetAndMetadata): Unit = {
    pendingOffsetCommits.get(topicPartition) match {
      case Some(pendingOffset) if offset == pendingOffset => pendingOffsetCommits.remove(topicPartition)
      case _ =>
    }
  }

  def prepareOffsetCommit(offsets: Map[TopicPartition, OffsetAndMetadata]) {
    receivedConsumerOffsetCommits = true
    pendingOffsetCommits ++= offsets
  }

  def prepareTxnOffsetCommit(producerId: Long, offsets: Map[TopicPartition, OffsetAndMetadata]) {
    trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offsets $offsets is pending")
    receivedTransactionalOffsetCommits = true
    val producerOffsets = pendingTransactionalOffsetCommits.getOrElseUpdate(producerId,
      mutable.Map.empty[TopicPartition, CommitRecordMetadataAndOffset])

    offsets.foreach { case (topicPartition, offsetAndMetadata) =>
      producerOffsets.put(topicPartition, CommitRecordMetadataAndOffset(None, offsetAndMetadata))
    }
  }

  def hasReceivedConsistentOffsetCommits : Boolean = {
    !receivedConsumerOffsetCommits || !receivedTransactionalOffsetCommits
  }

  /* Remove a pending transactional offset commit if the actual offset commit record was not written to the log.
   * We will return an error and the client will retry the request, potentially to a different coordinator.
   */
  def failPendingTxnOffsetCommit(producerId: Long, topicPartition: TopicPartition): Unit = {
    pendingTransactionalOffsetCommits.get(producerId) match {
      case Some(pendingOffsets) =>
        val pendingOffsetCommit = pendingOffsets.remove(topicPartition)
        trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offsets $pendingOffsetCommit failed " +
          s"to be appended to the log")
        if (pendingOffsets.isEmpty)
          pendingTransactionalOffsetCommits.remove(producerId)
      case _ =>
        // We may hit this case if the partition in question has emigrated already.
    }
  }

  def onTxnOffsetCommitAppend(producerId: Long, topicPartition: TopicPartition,
                              commitRecordMetadataAndOffset: CommitRecordMetadataAndOffset) {
    pendingTransactionalOffsetCommits.get(producerId) match {
      case Some(pendingOffset) =>
        if (pendingOffset.contains(topicPartition)
          && pendingOffset(topicPartition).offsetAndMetadata == commitRecordMetadataAndOffset.offsetAndMetadata)
          pendingOffset.update(topicPartition, commitRecordMetadataAndOffset)
      case _ =>
        // We may hit this case if the partition in question has emigrated.
    }
  }

  /* 完成待处理的事务性偏移量提交。 这是在提交或中止标记完全写入日志后调用的。
   */
  def completePendingTxnOffsetCommit(producerId: Long, isCommit: Boolean): Unit = {
    val pendingOffsetsOpt = pendingTransactionalOffsetCommits.remove(producerId)
    if (isCommit) {
      pendingOffsetsOpt.foreach { pendingOffsets =>
        pendingOffsets.foreach { case (topicPartition, commitRecordMetadataAndOffset) =>
          if (commitRecordMetadataAndOffset.appendedBatchOffset.isEmpty)
            throw new IllegalStateException(s"Trying to complete a transactional offset commit for producerId $producerId " +
              s"and groupId $groupId even though the offset commit record itself hasn't been appended to the log.")

          val currentOffsetOpt = offsets.get(topicPartition)
          if (currentOffsetOpt.forall(_.olderThan(commitRecordMetadataAndOffset))) {
            trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offset $commitRecordMetadataAndOffset " +
              "committed and loaded into the cache.")
            offsets.put(topicPartition, commitRecordMetadataAndOffset)
          } else {
            trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offset $commitRecordMetadataAndOffset " +
              s"committed, but not loaded since its offset is older than current offset $currentOffsetOpt.")
          }
        }
      }
    } else {
      trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offsets $pendingOffsetsOpt aborted")
    }
  }

  def activeProducers = pendingTransactionalOffsetCommits.keySet

  def hasPendingOffsetCommitsFromProducer(producerId: Long) =
    pendingTransactionalOffsetCommits.contains(producerId)

  def removeAllOffsets(): immutable.Map[TopicPartition, OffsetAndMetadata] = removeOffsets(offsets.keySet.toSeq)

  // 移除tp
  def removeOffsets(topicPartitions: Seq[TopicPartition]): immutable.Map[TopicPartition, OffsetAndMetadata] = {
    topicPartitions.flatMap { topicPartition =>
      pendingOffsetCommits.remove(topicPartition)
      pendingTransactionalOffsetCommits.foreach { case (_, pendingOffsets) =>
        pendingOffsets.remove(topicPartition)
      }
      val removedOffset = offsets.remove(topicPartition)
      removedOffset.map(topicPartition -> _.offsetAndMetadata)
    }.toMap
  }

  // 移除过期的偏移量
  def removeExpiredOffsets(startMs: Long) : Map[TopicPartition, OffsetAndMetadata] = {
    val expiredOffsets = offsets
      .filter {
        case (topicPartition, commitRecordMetadataAndOffset) => // 过期并且是等待提交的偏移量中未包含当前tp
          commitRecordMetadataAndOffset.offsetAndMetadata.expireTimestamp < startMs && !pendingOffsetCommits.contains(topicPartition)
      }
      .map {
        case (topicPartition, commitRecordOffsetAndMetadata) =>
          (topicPartition, commitRecordOffsetAndMetadata.offsetAndMetadata)
      }
    offsets --= expiredOffsets.keySet
    expiredOffsets.toMap
  }

  def allOffsets = offsets.map { case (topicPartition, commitRecordMetadataAndOffset) =>
    (topicPartition, commitRecordMetadataAndOffset.offsetAndMetadata)
  }.toMap

  def offset(topicPartition: TopicPartition): Option[OffsetAndMetadata] = offsets.get(topicPartition).map(_.offsetAndMetadata)

  // visible for testing
  private[group] def offsetWithRecordMetadata(topicPartition: TopicPartition): Option[CommitRecordMetadataAndOffset] = offsets.get(topicPartition)

  def numOffsets = offsets.size

  def hasOffsets = offsets.nonEmpty || pendingOffsetCommits.nonEmpty || pendingTransactionalOffsetCommits.nonEmpty

  private def assertValidTransition(targetState: GroupState) {
    if (!GroupMetadata.validPreviousStates(targetState).contains(state))
      throw new IllegalStateException("Group %s should be in the %s states before moving to %s state. Instead it is in %s state"
        .format(groupId, GroupMetadata.validPreviousStates(targetState).mkString(","), targetState, state))
  }

  override def toString: String = {
    "GroupMetadata(" +
      s"groupId=$groupId, " +
      s"generation=$generationId, " +
      s"protocolType=$protocolType, " +
      s"currentState=$currentState, " +
      s"members=$members)"
  }

}

