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
package kafka.coordinator.transaction

import java.util.concurrent.locks.ReentrantLock

import kafka.utils.{CoreUtils, Logging, nonthreadsafe}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.RecordBatch

import scala.collection.{immutable, mutable}

private[transaction] sealed trait TransactionState { def byte: Byte }

/**
 * Transaction has not existed yet
 *
 * transition: received AddPartitionsToTxnRequest => Ongoing
 *             received AddOffsetsToTxnRequest => Ongoing
 */
private[transaction] case object Empty extends TransactionState { val byte: Byte = 0 }

/**
 * Transaction has started and ongoing
 *
 * transition: received EndTxnRequest with commit => PrepareCommit
 *             received EndTxnRequest with abort => PrepareAbort
 *             received AddPartitionsToTxnRequest => Ongoing
 *             received AddOffsetsToTxnRequest => Ongoing
 */
private[transaction] case object Ongoing extends TransactionState { val byte: Byte = 1 }

/**
 * Group is preparing to commit
 *
 * transition: received acks from all partitions => CompleteCommit
 */
private[transaction] case object PrepareCommit extends TransactionState { val byte: Byte = 2}

/**
 * 集团正在准备中止
 *
 * transition: received acks from all partitions => CompleteAbort
 */
private[transaction] case object PrepareAbort extends TransactionState { val byte: Byte = 3 }

/**
 * 组已完成提交
 * 将很快从正在进行的事务缓存中删除
 */
private[transaction] case object CompleteCommit extends TransactionState { val byte: Byte = 4 }

/**
 * 小组已完成中止
 *
 * 将很快从正在进行的事务缓存中删除
 */
private[transaction] case object CompleteAbort extends TransactionState { val byte: Byte = 5 }

/**
  * TransactionalId已过期，即将从事务高速缓存中删除
  */
private[transaction] case object Dead extends TransactionState { val byte: Byte = 6 }

/**
  * 我们正在冲击这个epoch，并围绕着老的生产者。
  */
private[transaction] case object PrepareEpochFence extends TransactionState { val byte: Byte = 7}

private[transaction] object TransactionMetadata {
  def apply(transactionalId: String, producerId: Long, producerEpoch: Short, txnTimeoutMs: Int, timestamp: Long) =
    new TransactionMetadata(transactionalId, producerId, producerEpoch, txnTimeoutMs, Empty,
      collection.mutable.Set.empty[TopicPartition], timestamp, timestamp)

  def apply(transactionalId: String, producerId: Long, producerEpoch: Short, txnTimeoutMs: Int,
            state: TransactionState, timestamp: Long) =
    new TransactionMetadata(transactionalId, producerId, producerEpoch, txnTimeoutMs, state,
      collection.mutable.Set.empty[TopicPartition], timestamp, timestamp)

  def byteToState(byte: Byte): TransactionState = {
    byte match {
      case 0 => Empty
      case 1 => Ongoing
      case 2 => PrepareCommit
      case 3 => PrepareAbort
      case 4 => CompleteCommit
      case 5 => CompleteAbort
      case 6 => Dead
      case 7 => PrepareEpochFence
      case unknown => throw new IllegalStateException("Unknown transaction state byte " + unknown + " from the transaction status message")
    }
  }

  def isValidTransition(oldState: TransactionState, newState: TransactionState): Boolean =
    TransactionMetadata.validPreviousStates(newState).contains(oldState)

  private val validPreviousStates: Map[TransactionState, Set[TransactionState]] =
    Map(Empty -> Set(Empty, CompleteCommit, CompleteAbort),
      Ongoing -> Set(Ongoing, Empty, CompleteCommit, CompleteAbort),
      PrepareCommit -> Set(Ongoing),
      PrepareAbort -> Set(Ongoing, PrepareEpochFence),
      CompleteCommit -> Set(PrepareCommit),
      CompleteAbort -> Set(PrepareAbort),
      Dead -> Set(Empty, CompleteAbort, CompleteCommit),
      PrepareEpochFence -> Set(Ongoing)
    )
}

// 这是表示事务元数据的目标转换的不可变对象
private[transaction] case class TxnTransitMetadata(producerId: Long,
                                                   producerEpoch: Short,
                                                   txnTimeoutMs: Int,
                                                   txnState: TransactionState,
                                                   topicPartitions: immutable.Set[TopicPartition],
                                                   txnStartTimestamp: Long,
                                                   txnLastUpdateTimestamp: Long) {
  override def toString: String = {
    "TxnTransitMetadata(" +
      s"producerId=$producerId, " +
      s"producerEpoch=$producerEpoch, " +
      s"txnTimeoutMs=$txnTimeoutMs, " +
      s"txnState=$txnState, " +
      s"topicPartitions=$topicPartitions, " +
      s"txnStartTimestamp=$txnStartTimestamp, " +
      s"txnLastUpdateTimestamp=$txnLastUpdateTimestamp)"
  }
}

/**
  * 事务的元数据组成
  * @param producerId            producer id
  * @param producerEpoch         current epoch of the producer
  * @param txnTimeoutMs          timeout to be used to abort long running transactions
  * @param state                 current state of the transaction
  * @param topicPartitions       current set of partitions that are part of this transaction
  * @param txnStartTimestamp     time the transaction was started, i.e., when first partition is added
  * @param txnLastUpdateTimestamp   updated when any operation updates the TransactionMetadata. To be used for expiration
  */
@nonthreadsafe
private[transaction] class TransactionMetadata(val transactionalId: String,
                                               var producerId: Long,
                                               var producerEpoch: Short,
                                               var txnTimeoutMs: Int,
                                               var state: TransactionState,
                                               val topicPartitions: mutable.Set[TopicPartition],
                                               @volatile var txnStartTimestamp: Long = -1,
                                               @volatile var txnLastUpdateTimestamp: Long) extends Logging {

  // 挂起状态用于指示此事务将要转移到的状态，并且用于阻止未来尝试再次转移它，如果它不合法的话;将其初始化为与当前状态相同
  var pendingState: Option[TransactionState] = None

  private[transaction] val lock = new ReentrantLock

  def inLock[T](fun: => T): T = CoreUtils.inLock(lock)(fun)

  def addPartitions(partitions: collection.Set[TopicPartition]): Unit = {
    topicPartitions ++= partitions
  }

  def removePartition(topicPartition: TopicPartition): Unit = {
    if (state != PrepareCommit && state != PrepareAbort)
      throw new IllegalStateException(s"Transaction metadata's current state is $state, and its pending state is $pendingState " +
        s"while trying to remove partitions whose txn marker has been sent, this is not expected")

    topicPartitions -= topicPartition
  }

  // this is visible for test only
  def prepareNoTransit(): TxnTransitMetadata = {
    // do not call transitTo as it will set the pending state, a follow-up call to abort the transaction will set its pending state
    TxnTransitMetadata(producerId, producerEpoch, txnTimeoutMs, state, topicPartitions.toSet, txnStartTimestamp, txnLastUpdateTimestamp)
  }

  def prepareFenceProducerEpoch(): TxnTransitMetadata = {
    if (producerEpoch == Short.MaxValue)
      throw new IllegalStateException(s"Cannot fence producer with epoch equal to Short.MaxValue since this would overflow")

    prepareTransitionTo(PrepareEpochFence, producerId, (producerEpoch + 1).toShort /** 增加生产者epoch */, txnTimeoutMs, topicPartitions.toSet,
      txnStartTimestamp, txnLastUpdateTimestamp)
  }

  def prepareIncrementProducerEpoch(newTxnTimeoutMs: Int, updateTimestamp: Long): TxnTransitMetadata = {
    if (isProducerEpochExhausted)
      throw new IllegalStateException(s"Cannot allocate any more producer epochs for producerId $producerId")

    val nextEpoch = if (producerEpoch == RecordBatch.NO_PRODUCER_EPOCH) 0 else producerEpoch + 1
    prepareTransitionTo(Empty, producerId, nextEpoch.toShort, newTxnTimeoutMs, immutable.Set.empty[TopicPartition], -1,
      updateTimestamp)
  }

  def prepareProducerIdRotation(newProducerId: Long, newTxnTimeoutMs: Int, updateTimestamp: Long): TxnTransitMetadata = {
    if (hasPendingTransaction)
      throw new IllegalStateException("Cannot rotate producer ids while a transaction is still pending")
    prepareTransitionTo(Empty, newProducerId, 0, newTxnTimeoutMs, immutable.Set.empty[TopicPartition], -1, updateTimestamp)
  }

  def prepareAddPartitions(addedTopicPartitions: immutable.Set[TopicPartition], updateTimestamp: Long): TxnTransitMetadata = {
    val newTxnStartTimestamp = state match {  // 新的事务时间
      case Empty | CompleteAbort | CompleteCommit => updateTimestamp
      case _ => txnStartTimestamp
    }

    // 添加新的分区
    prepareTransitionTo(Ongoing, producerId, producerEpoch, txnTimeoutMs, (topicPartitions ++ addedTopicPartitions).toSet,
      newTxnStartTimestamp, updateTimestamp)
  }

  // 过渡（预）中断或者完成
  def prepareAbortOrCommit(newState: TransactionState, updateTimestamp: Long): TxnTransitMetadata = {
    prepareTransitionTo(newState, producerId, producerEpoch, txnTimeoutMs, topicPartitions.toSet, txnStartTimestamp,
      updateTimestamp)
  }

  // 向事务完成过渡
  def prepareComplete(updateTimestamp: Long): TxnTransitMetadata = {
    val newState = if (state == PrepareCommit) CompleteCommit else CompleteAbort
    prepareTransitionTo(newState, producerId, producerEpoch, txnTimeoutMs, Set.empty[TopicPartition], txnStartTimestamp,
      updateTimestamp)
  }

  // 转换为死亡状态
  def prepareDead(): TxnTransitMetadata = {
    prepareTransitionTo(Dead, producerId, producerEpoch, txnTimeoutMs, Set.empty[TopicPartition], txnStartTimestamp,
      txnLastUpdateTimestamp)
  }

  /**
    * 检查当前producerId是否耗尽了这些纪元。
    * 我们不允许客户端使用与Short.MaxValue相等的历元，以确保协调员始终能够对现有生产者进行篱笆划分。
   */
  def isProducerEpochExhausted: Boolean = producerEpoch >= Short.MaxValue - 1

  private def hasPendingTransaction: Boolean = {
    state match {
      case Ongoing | PrepareAbort | PrepareCommit => true
      case _ => false
    }
  }

  // 事务转换的预处理
  private def prepareTransitionTo(newState: TransactionState,
                                  newProducerId: Long,
                                  newEpoch: Short,
                                  newTxnTimeoutMs: Int,
                                  newTopicPartitions: immutable.Set[TopicPartition],
                                  newTxnStartTimestamp: Long,
                                  updateTimestamp: Long): TxnTransitMetadata = {
    if (pendingState.isDefined)
      throw new IllegalStateException(s"Preparing transaction state transition to $newState " +
        s"while it already a pending state ${pendingState.get}")

    if (newProducerId < 0)
      throw new IllegalArgumentException(s"Illegal new producer id $newProducerId")

    if (newEpoch < 0)
      throw new IllegalArgumentException(s"Illegal new producer epoch $newEpoch")

    // 检查新的状态转换是否有效，并在必要时更新暂挂状态
    if (TransactionMetadata.validPreviousStates(newState).contains(state)) {
      val transitMetadata = TxnTransitMetadata(newProducerId, newEpoch, newTxnTimeoutMs, newState,
        newTopicPartitions, newTxnStartTimestamp, updateTimestamp)
      debug(s"TransactionalId $transactionalId prepare transition from $state to $transitMetadata")
      pendingState = Some(newState)
      transitMetadata
    } else {
      throw new IllegalStateException(s"Preparing transaction state transition to $newState failed since the target state" +
        s" $newState is not a valid previous state of the current state $state")
    }
  }

  def completeTransitionTo(transitMetadata: TxnTransitMetadata): Unit = {
    // 只有满足以下所有条件，元数据转换才有效：
    //
    // 1.新状态已经显示在待处理状态。
    // 2.时期应该是相同的值，旧的值+1，或者如果我们有一个新的producerId，则为0。
    // 3.最后更新时间不小于旧值。
    // 4.旧分区集是新分区集的一个子集。
    //
    // 另外，我们应该只尝试在相应日志条目成功后更新元数据
    // 写和复制（请参阅TransactionStateManager＃appendTransactionToLog）
    //
    // 如果有效，则通过覆盖整个对象来完成转换以确保同步
    val toState = pendingState.getOrElse {
      fatal(s"$this's transition to $transitMetadata failed since pendingState is not defined: this should not happen")

      throw new IllegalStateException(s"TransactionalId $transactionalId " +
        "completing transaction state transition while it does not have a pending state")
    }

    if (toState != transitMetadata.txnState) { // 不符合预期，抛出异常
      throwStateTransitionFailure(transitMetadata)
    } else {
      toState match {
        case Empty => // from initPid
          if ((producerEpoch != transitMetadata.producerEpoch && !validProducerEpochBump(transitMetadata)) ||
            transitMetadata.topicPartitions.nonEmpty ||
            transitMetadata.txnStartTimestamp != -1) { // 不符合要求

            throwStateTransitionFailure(transitMetadata)
          } else {
            txnTimeoutMs = transitMetadata.txnTimeoutMs
            producerEpoch = transitMetadata.producerEpoch
            producerId = transitMetadata.producerId
          }

        case Ongoing => // from addPartitions
          if (!validProducerEpoch(transitMetadata) ||
            !topicPartitions.subsetOf(transitMetadata.topicPartitions) ||
            txnTimeoutMs != transitMetadata.txnTimeoutMs ||
            txnStartTimestamp > transitMetadata.txnStartTimestamp) {

            throwStateTransitionFailure(transitMetadata)
          } else {
            txnStartTimestamp = transitMetadata.txnStartTimestamp
            addPartitions(transitMetadata.topicPartitions)
          }

        case PrepareAbort | PrepareCommit => // from endTxn
          if (!validProducerEpoch(transitMetadata) ||
            !topicPartitions.toSet.equals(transitMetadata.topicPartitions) ||
            txnTimeoutMs != transitMetadata.txnTimeoutMs ||
            txnStartTimestamp != transitMetadata.txnStartTimestamp) {

            throwStateTransitionFailure(transitMetadata)
          }

        case CompleteAbort | CompleteCommit => // from write markers
          if (!validProducerEpoch(transitMetadata) ||
            txnTimeoutMs != transitMetadata.txnTimeoutMs ||
            transitMetadata.txnStartTimestamp == -1) {

            throwStateTransitionFailure(transitMetadata)
          } else {
            txnStartTimestamp = transitMetadata.txnStartTimestamp
            topicPartitions.clear()
          }

        case PrepareEpochFence =>
          // We should never get here, since once we prepare to fence the epoch, we immediately set the pending state
          // to PrepareAbort, and then consequently to CompleteAbort after the markers are written.. So we should never
          // ever try to complete a transition to PrepareEpochFence, as it is not a valid previous state for any other state, and hence
          // can never be transitioned out of.
          throwStateTransitionFailure(transitMetadata)


        case Dead => // 事务清除
          // The transactionalId was being expired. The completion of the operation should result in removal of the
          // the metadata from the cache, so we should never realistically transition to the dead state.
          throw new IllegalStateException(s"TransactionalId $transactionalId is trying to complete a transition to " +
            s"$toState. This means that the transactionalId was being expired, and the only acceptable completion of " +
            s"this operation is to remove the transaction metadata from the cache, not to persist the $toState in the log.")
      }

      debug(s"TransactionalId $transactionalId complete transition from $state to $transitMetadata")
      txnLastUpdateTimestamp = transitMetadata.txnLastUpdateTimestamp
      // 将过渡状态进行转换
      pendingState = None
      state = toState
    }
  }

  private def validProducerEpoch(transitMetadata: TxnTransitMetadata): Boolean = {
    val transitEpoch = transitMetadata.producerEpoch
    val transitProducerId = transitMetadata.producerId
    transitEpoch == producerEpoch && transitProducerId == producerId
  }

  private def validProducerEpochBump(transitMetadata: TxnTransitMetadata): Boolean = {
    val transitEpoch = transitMetadata.producerEpoch
    val transitProducerId = transitMetadata.producerId
    transitEpoch == producerEpoch + 1 || (transitEpoch == 0 && transitProducerId != producerId)
  }

  private def throwStateTransitionFailure(txnTransitMetadata: TxnTransitMetadata): Unit = {
    fatal(s"${this.toString}'s transition to $txnTransitMetadata failed: this should not happen")

    throw new IllegalStateException(s"TransactionalId $transactionalId failed transition to state $txnTransitMetadata " +
      "due to unexpected metadata")
  }

  def pendingTransitionInProgress: Boolean = pendingState.isDefined

  override def toString: String = {
    "TransactionMetadata(" +
      s"transactionalId=$transactionalId, " +
      s"producerId=$producerId, " +
      s"producerEpoch=$producerEpoch, " +
      s"txnTimeoutMs=$txnTimeoutMs, " +
      s"state=$state, " +
      s"pendingState=$pendingState, " +
      s"topicPartitions=$topicPartitions, " +
      s"txnStartTimestamp=$txnStartTimestamp, " +
      s"txnLastUpdateTimestamp=$txnLastUpdateTimestamp)"
  }

  override def equals(that: Any): Boolean = that match {
    case other: TransactionMetadata =>
      transactionalId == other.transactionalId &&
      producerId == other.producerId &&
      producerEpoch == other.producerEpoch &&
      txnTimeoutMs == other.txnTimeoutMs &&
      state.equals(other.state) &&
      topicPartitions.equals(other.topicPartitions) &&
      txnStartTimestamp == other.txnStartTimestamp &&
      txnLastUpdateTimestamp == other.txnLastUpdateTimestamp
    case _ => false
  }

  override def hashCode(): Int = {
    val fields = Seq(transactionalId, producerId, producerEpoch, txnTimeoutMs, state, topicPartitions,
      txnStartTimestamp, txnLastUpdateTimestamp)
    fields.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
