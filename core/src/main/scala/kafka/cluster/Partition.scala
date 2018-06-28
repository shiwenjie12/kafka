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
package kafka.cluster


import java.util.concurrent.locks.ReentrantReadWriteLock

import com.yammer.metrics.core.Gauge
import kafka.api.LeaderAndIsr
import kafka.api.Request
import kafka.controller.KafkaController
import kafka.log.{LogAppendInfo, LogConfig}
import kafka.metrics.KafkaMetricsGroup
import kafka.server._
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.utils._
import kafka.zk.AdminZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{NotEnoughReplicasException, NotLeaderForPartitionException, PolicyViolationException}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.Errors._
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.EpochEndOffset._
import org.apache.kafka.common.requests.{EpochEndOffset, LeaderAndIsrRequest}
import org.apache.kafka.common.utils.Time

import scala.collection.JavaConverters._
import scala.collection.Map

/**
 * 表示主题分区的数据结构。领导者保持AR，ISR，CURR，RAR
 */
class Partition(val topic: String,
                val partitionId: Int,
                time: Time,
                replicaManager: ReplicaManager,
                val isOffline: Boolean = false) extends Logging with KafkaMetricsGroup {

  val topicPartition = new TopicPartition(topic, partitionId)

  // Do not use replicaManager if this partition is ReplicaManager.OfflinePartition
  private val localBrokerId = if (!isOffline) replicaManager.config.brokerId else -1
  private val logManager = if (!isOffline) replicaManager.logManager else null
  private val zkClient = if (!isOffline) replicaManager.zkClient else null
  // 如果正在进行副本移动，allReplicasMap包括分配的副本和将来的副本
  private val allReplicasMap = new Pool[Int, Replica]
  // 读锁只在执行多个读并且需要一致的方式时才需要。
  private val leaderIsrUpdateLock = new ReentrantReadWriteLock
  private var zkVersion: Int = LeaderAndIsr.initialZKVersion
  @volatile private var leaderEpoch: Int = LeaderAndIsr.initialLeaderEpoch - 1
  // 领导者的副本id
  @volatile var leaderReplicaIdOpt: Option[Int] = None
  // 处于同步队列的副本
  @volatile var inSyncReplicas: Set[Replica] = Set.empty[Replica]

  /* Epoch of the controller that last changed the leader. This needs to be initialized correctly upon broker startup.
   * One way of doing that is through the controller's start replica state change command. When a new broker starts up
   * the controller sends it a start replica command containing the leader for each partition that the broker hosts.
   * In addition to the leader, the controller can also send the epoch of the controller that elected the leader for
   * each partition. */
  private var controllerEpoch: Int = KafkaController.InitialControllerEpoch - 1
  this.logIdent = s"[Partition $topicPartition broker=$localBrokerId] "

  private def isReplicaLocal(replicaId: Int) : Boolean = replicaId == localBrokerId || replicaId == Request.FutureLocalReplicaId

  private val tags = Map("topic" -> topic, "partition" -> partitionId.toString)

  // Do not create metrics if this partition is ReplicaManager.OfflinePartition
  if (!isOffline) {
    newGauge("UnderReplicated",
      new Gauge[Int] {
        def value = {
          if (isUnderReplicated) 1 else 0
        }
      },
      tags
    )

    newGauge("InSyncReplicasCount",
      new Gauge[Int] {
        def value = {
          if (isLeaderReplicaLocal) inSyncReplicas.size else 0
        }
      },
      tags
    )

    newGauge("UnderMinIsr",
      new Gauge[Int] {
        def value = {
          if (isUnderMinIsr) 1 else 0
        }
      },
      tags
    )

    newGauge("ReplicasCount",
      new Gauge[Int] {
        def value = {
          if (isLeaderReplicaLocal) assignedReplicas.size else 0
        }
      },
      tags
    )

    newGauge("LastStableOffsetLag",
      new Gauge[Long] {
        def value = {
          leaderReplicaIfLocal.map { replica =>
            replica.highWatermark.messageOffset - replica.lastStableOffset.messageOffset
          }.getOrElse(0)
        }
      },
      tags
    )
  }

  private def isLeaderReplicaLocal: Boolean = leaderReplicaIfLocal.isDefined

  // 是否还在线下的分区
  def isUnderReplicated: Boolean =
    isLeaderReplicaLocal && inSyncReplicas.size < assignedReplicas.size

  // 是否在小于最小同步个数
  def isUnderMinIsr: Boolean = {
    leaderReplicaIfLocal match {
      case Some(leaderReplica) =>
        inSyncReplicas.size < leaderReplica.log.get.config.minInSyncReplicas
      case None =>
        false
    }
  }

  /**
    * Create the future replica if 1) the current replica is not in the given log directory and 2) the future replica
    * does not exist. This method assumes that the current replica has already been created.
    *
    * @param logDir log directory
    * @return true iff the future replica is created
    */
  def maybeCreateFutureReplica(logDir: String): Boolean = {
    // The readLock is needed to make sure that while the caller checks the log directory of the
    // current replica and the existence of the future replica, no other thread can update the log directory of the
    // current replica or remove the future replica.
    inReadLock(leaderIsrUpdateLock) {
      val currentReplica = getReplica().get
      if (currentReplica.log.get.dir.getParent == logDir)
        false
      else if (getReplica(Request.FutureLocalReplicaId).isDefined) {
        val futureReplicaLogDir = getReplica(Request.FutureLocalReplicaId).get.log.get.dir.getParent
        if (futureReplicaLogDir != logDir)
          throw new IllegalStateException(s"The future log dir $futureReplicaLogDir of $topicPartition is different from the requested log dir $logDir")
        false
      } else {
        getOrCreateReplica(Request.FutureLocalReplicaId)
        true
      }
    }
  }

  def getOrCreateReplica(replicaId: Int = localBrokerId, isNew: Boolean = false): Replica = {
    allReplicasMap.getAndMaybePut(replicaId, {
      if (isReplicaLocal(replicaId)) {
        val adminZkClient = new AdminZkClient(zkClient)
        val props = adminZkClient.fetchEntityConfig(ConfigType.Topic, topic)
        val config = LogConfig.fromProps(logManager.currentDefaultConfig.originals, props)
        val log = logManager.getOrCreateLog(topicPartition, config, isNew, replicaId == Request.FutureLocalReplicaId)
        val checkpoint = replicaManager.highWatermarkCheckpoints(log.dir.getParent)
        val offsetMap = checkpoint.read()
        if (!offsetMap.contains(topicPartition))
          info(s"No checkpointed highwatermark is found for partition $topicPartition")
        val offset = math.min(offsetMap.getOrElse(topicPartition, 0L), log.logEndOffset)
        new Replica(replicaId, topicPartition, time, offset, Some(log))
      } else new Replica(replicaId, topicPartition, time)
    })
  }

  def getReplica(replicaId: Int = localBrokerId): Option[Replica] = Option(allReplicasMap.get(replicaId))

  // 领导者（本地）副本
  def leaderReplicaIfLocal: Option[Replica] =
    leaderReplicaIdOpt.filter(_ == localBrokerId).flatMap(getReplica)

  def addReplicaIfNotExists(replica: Replica): Replica =
    allReplicasMap.putIfNotExists(replica.brokerId, replica)

  // 已经分配的分区
  def assignedReplicas: Set[Replica] =
    allReplicasMap.values.filter(replica => Request.isValidBrokerId(replica.brokerId)).toSet

  def allReplicas: Set[Replica] =
    allReplicasMap.values.toSet

  private def removeReplica(replicaId: Int) {
    allReplicasMap.remove(replicaId)
  }

  def removeFutureLocalReplica() {
    inWriteLock(leaderIsrUpdateLock) {
      allReplicasMap.remove(Request.FutureLocalReplicaId)
    }
  }

  // Return true iff the future log has caught up with the current log for this partition
  // Only ReplicaAlterDirThread will call this method and ReplicaAlterDirThread should remove the partition
  // from its partitionStates if this method returns true
  def maybeReplaceCurrentWithFutureReplica(): Boolean = {
    val replica = getReplica().get
    val futureReplica = getReplica(Request.FutureLocalReplicaId).get
    if (replica.logEndOffset == futureReplica.logEndOffset) {
      // The write lock is needed to make sure that while ReplicaAlterDirThread checks the LEO of the
      // current replica, no other thread can update LEO of the current replica via log truncation or log append operation.
      inWriteLock(leaderIsrUpdateLock) {
        if (replica.logEndOffset == futureReplica.logEndOffset) {
          logManager.replaceCurrentWithFutureLog(topicPartition)
          replica.log = futureReplica.log
          futureReplica.log = None
          allReplicasMap.remove(Request.FutureLocalReplicaId)
          true
        } else false
      }
    } else false
  }

  def delete() {
    // need to hold the lock to prevent appendMessagesToLeader() from hitting I/O exceptions due to log being deleted
    inWriteLock(leaderIsrUpdateLock) {
      allReplicasMap.clear()
      inSyncReplicas = Set.empty[Replica]
      leaderReplicaIdOpt = None
      removePartitionMetrics()
      logManager.asyncDelete(topicPartition)
      logManager.asyncDelete(topicPartition, isFuture = true)
    }
  }

  def getLeaderEpoch: Int = this.leaderEpoch

  /**
   * Make the local replica the leader by resetting LogEndOffset for remote replicas (there could be old LogEndOffset
   * from the time when this broker was the leader last time) and setting the new leader and ISR.
   * If the leader replica id does not change, return false to indicate the replica manager.
   */
  def makeLeader(controllerId: Int, partitionStateInfo: LeaderAndIsrRequest.PartitionState, correlationId: Int): Boolean = {
    val (leaderHWIncremented, isNewLeader) = inWriteLock(leaderIsrUpdateLock) {
      val newAssignedReplicas = partitionStateInfo.basePartitionState.replicas.asScala.map(_.toInt)
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path
      controllerEpoch = partitionStateInfo.basePartitionState.controllerEpoch
      // add replicas that are new
      val newInSyncReplicas = partitionStateInfo.basePartitionState.isr.asScala.map(r => getOrCreateReplica(r, partitionStateInfo.isNew)).toSet
      // remove assigned replicas that have been removed by the controller
      (assignedReplicas.map(_.brokerId) -- newAssignedReplicas).foreach(removeReplica)
      inSyncReplicas = newInSyncReplicas

      info(s"$topicPartition starts at Leader Epoch ${partitionStateInfo.basePartitionState.leaderEpoch} from offset ${getReplica().get.logEndOffset.messageOffset}. Previous Leader Epoch was: $leaderEpoch")

      //We cache the leader epoch here, persisting it only if it's local (hence having a log dir)
      leaderEpoch = partitionStateInfo.basePartitionState.leaderEpoch
      newAssignedReplicas.foreach(id => getOrCreateReplica(id, partitionStateInfo.isNew))

      zkVersion = partitionStateInfo.basePartitionState.zkVersion
      val isNewLeader = leaderReplicaIdOpt.map(_ != localBrokerId).getOrElse(true)

      val leaderReplica = getReplica().get
      val curLeaderLogEndOffset = leaderReplica.logEndOffset.messageOffset
      val curTimeMs = time.milliseconds
      // initialize lastCaughtUpTime of replicas as well as their lastFetchTimeMs and lastFetchLeaderLogEndOffset.
      (assignedReplicas - leaderReplica).foreach { replica =>
        val lastCaughtUpTimeMs = if (inSyncReplicas.contains(replica)) curTimeMs else 0L
        replica.resetLastCaughtUpTime(curLeaderLogEndOffset, curTimeMs, lastCaughtUpTimeMs)
      }

      if (isNewLeader) {
        // construct the high watermark metadata for the new leader replica
        leaderReplica.convertHWToLocalOffsetMetadata()
        // mark local replica as the leader after converting hw
        leaderReplicaIdOpt = Some(localBrokerId)
        // reset log end offset for remote replicas
        assignedReplicas.filter(_.brokerId != localBrokerId).foreach(_.updateLogReadResult(LogReadResult.UnknownLogReadResult))
      }
      // we may need to increment high watermark since ISR could be down to 1
      (maybeIncrementLeaderHW(leaderReplica), isNewLeader)
    }
    // some delayed operations may be unblocked after HW changed
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()
    isNewLeader
  }

  /**
   *  Make the local replica the follower by setting the new leader and ISR to empty
   *  If the leader replica id does not change, return false to indicate the replica manager
   */
  def makeFollower(controllerId: Int, partitionStateInfo: LeaderAndIsrRequest.PartitionState, correlationId: Int): Boolean = {
    inWriteLock(leaderIsrUpdateLock) {
      val newAssignedReplicas = partitionStateInfo.basePartitionState.replicas.asScala.map(_.toInt)
      val newLeaderBrokerId: Int = partitionStateInfo.basePartitionState.leader
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path
      controllerEpoch = partitionStateInfo.basePartitionState.controllerEpoch
      // add replicas that are new
      newAssignedReplicas.foreach(r => getOrCreateReplica(r, partitionStateInfo.isNew))
      // remove assigned replicas that have been removed by the controller
      (assignedReplicas.map(_.brokerId) -- newAssignedReplicas).foreach(removeReplica)
      inSyncReplicas = Set.empty[Replica]
      leaderEpoch = partitionStateInfo.basePartitionState.leaderEpoch
      zkVersion = partitionStateInfo.basePartitionState.zkVersion

      if (leaderReplicaIdOpt.isDefined && leaderReplicaIdOpt.get == newLeaderBrokerId) {
        false
      }
      else {
        leaderReplicaIdOpt = Some(newLeaderBrokerId)
        true
      }
    }
  }

  /**
   * 根据最后一次获取请求更新领导者的追随者状态。 See
   * [[kafka.cluster.Replica#updateLogReadResult]] for details.
   *
   * @return 如果领导者的日志起始偏移或高水位已更新，则为true
   */
  def updateReplicaLogReadResult(replica: Replica, logReadResult: LogReadResult): Boolean = {
    val replicaId = replica.brokerId
    // 如果没有延迟的DeleteRecordsRequest，则不需要计算低水印
    val oldLeaderLW = if (replicaManager.delayedDeleteRecordsPurgatory.delayed > 0) lowWatermarkIfLeader else -1L
    replica.updateLogReadResult(logReadResult)
    val newLeaderLW = if (replicaManager.delayedDeleteRecordsPurgatory.delayed > 0) lowWatermarkIfLeader else -1L
    // 检查分区的LW是否已增加，因为副本的logStartOffset可能已递增
    val leaderLWIncremented = newLeaderLW > oldLeaderLW
    // 检查我们是否需要扩展ISR以包含此副本，如果它不在ISR中
    val leaderHWIncremented = maybeExpandIsr(replicaId, logReadResult)

    val result = leaderLWIncremented || leaderHWIncremented
    // 一些延迟的操作可能在HW或LW改变后解除阻塞
    if (result)
      tryCompleteDelayedRequests()

    debug(s"Recorded replica $replicaId log end offset (LEO) position ${logReadResult.info.fetchOffsetMetadata.messageOffset}.")
    result
  }

  /**
   * 检查并可能扩展分区的ISR。
   * 如果其LEO> =分区的当前hw，副本将被添加到ISR。
   *
   * 从技术上讲，如果复制品没有追上比replicaLagTimeMaxMs更长的时间，则不应该在ISR中，
   * 即使它的日志结束偏移是> = HW。 但要与追随者的决定保持一致
   * 不管副本是否同步，我们只检查硬件。
   * @return true if the high watermark has been updated
   */
  def maybeExpandIsr(replicaId: Int, logReadResult: LogReadResult): Boolean = {
    inWriteLock(leaderIsrUpdateLock) {
      // 检查是否需要将此副本添加到ISR
      leaderReplicaIfLocal match {
        case Some(leaderReplica) =>
          val replica = getReplica(replicaId).get
          val leaderHW = leaderReplica.highWatermark
          // 满足条件直接添加，因为还有一个紧缩ISR的线程
          if (!inSyncReplicas.contains(replica) &&  // ISR未包含
             assignedReplicas.map(_.brokerId).contains(replicaId) && // 在分配的分区包含
             replica.logEndOffset.offsetDiff(leaderHW) >= 0) {
            val newInSyncReplicas = inSyncReplicas + replica
            info(s"Expanding ISR from ${inSyncReplicas.map(_.brokerId).mkString(",")} " +
              s"to ${newInSyncReplicas.map(_.brokerId).mkString(",")}")
            // update ISR in ZK and cache
            updateIsr(newInSyncReplicas)
            replicaManager.isrExpandRate.mark()
          }
          // 检查分区的HW现在是否可以增加，因为副本可能已经在ISR中，并且它的LEO刚增加
          maybeIncrementLeaderHW(leaderReplica, logReadResult.fetchTimeMs)
        case None => false // nothing to do if no longer leader
      }
    }
  }

  /*
   * 返回一个元组，其中第一个元素是一个布尔值，指示是否有足够的副本达到'requiredOffset'
   * 第二个元素是一个错误（如果没有错误，这将是`Errors.NONE`）。
   *
   * 请注意，只有在requiredAcks = -1时才会调用此方法，并且我们正在等待ISR中的所有副本
   * 完全赶上（本地）领导者的偏移量，相应于此产生请求，然后我们承认这一点
   * 产生请求。
   */
  def checkEnoughReplicasReachOffset(requiredOffset: Long): (Boolean, Errors) = {
    leaderReplicaIfLocal match {
      case Some(leaderReplica) =>
        // 保持当前不可变副本列表引用
        val curInSyncReplicas = inSyncReplicas

        def numAcks = curInSyncReplicas.count { r => // 表示达到需求的备份数
          if (!r.isLocal)
            if (r.logEndOffset.messageOffset >= requiredOffset) {
              trace(s"Replica ${r.brokerId} received offset $requiredOffset")
              true
            }
            else
              false
          else
            true /* also count the local (leader) replica */
        }

        trace(s"$numAcks acks satisfied with acks = -1")

        val minIsr = leaderReplica.log.get.config.minInSyncReplicas // 最小的同步数

        if (leaderReplica.highWatermark.messageOffset >= requiredOffset) { // hw大于必须的偏移量
          /*
           * 如果在ISR中没有足够的复制副本，则该主题可能被配置为不接受消息，
           * 在这种情况下，请求已经附加在本地，然后在ISR收缩之前添加到炼狱中
           */
          if (minIsr <= curInSyncReplicas.size)
            (true, Errors.NONE)
          else
            (true, Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND)
        } else
          (false, Errors.NONE)
      case None =>
        (false, Errors.NOT_LEADER_FOR_PARTITION)
    }
  }

  /**
    * 检查并可能增加分区的高水位;
    * 此功能可在触发时触发
    * 1.分区ISR改变了
    * 2.任何副本的LEO都已更改
   *
   * The HW is determined by the smallest log end offset among all replicas that are in sync or are considered caught-up.
   * This way, if a replica is considered caught-up, but its log end offset is smaller than HW, we will wait for this
   * replica to catch up to the HW before advancing the HW. This helps the situation when the ISR only includes the
   * leader replica and a follower tries to catch up. If we don't wait for the follower when advancing the HW, the
   * follower's log end offset may keep falling behind the HW (determined by the leader's log end offset) and therefore
   * will never be added to ISR.
   *
   * Returns true if the HW was incremented, and false otherwise.
   * Note There is no need to acquire the leaderIsrUpdate lock here
   * since all callers of this private API acquire that lock
   */
  private def maybeIncrementLeaderHW(leaderReplica: Replica, curTime: Long = time.milliseconds): Boolean = {
    // 在ISR中或者捕捉时间小于延迟时间 的副本
    val allLogEndOffsets = assignedReplicas.filter { replica =>
      curTime - replica.lastCaughtUpTimeMs <= replicaManager.config.replicaLagTimeMaxMs || inSyncReplicas.contains(replica)
    }.map(_.logEndOffset)
    val newHighWatermark = allLogEndOffsets.min(new LogOffsetMetadata.OffsetOrdering)
    val oldHighWatermark = leaderReplica.highWatermark
    if (oldHighWatermark.messageOffset < newHighWatermark.messageOffset || oldHighWatermark.onOlderSegment(newHighWatermark)) {
      leaderReplica.highWatermark = newHighWatermark   // 更新新的hw
      debug(s"High watermark updated to $newHighWatermark")
      true
    } else  {
      debug(s"Skipping update high watermark since new hw $newHighWatermark is not larger than old hw $oldHighWatermark." +
        s"All LEOs are ${allLogEndOffsets.mkString(",")}")
      false
    }
  }

  /**
    * 低位水印偏移值，仅在本地副本是分区头部时计算它仅由领导代理用于决定何时满足DeleteRecordsRequest。
    * 它的值是所有活动副本的最小logStartOffset当领导代理收到FetchRequest或DeleteRecordsRequest时，低水位将增加。
   */
  def lowWatermarkIfLeader: Long = {
    if (!isLeaderReplicaLocal)
      throw new NotLeaderForPartitionException("Leader not local for partition %s on broker %d".format(topicPartition, localBrokerId))
    val logStartOffsets = allReplicas.collect { // 所有可用分区的开始偏移量
      case replica if replicaManager.metadataCache.isBrokerAlive(replica.brokerId) || replica.brokerId == Request.FutureLocalReplicaId => replica.logStartOffset
    }
    CoreUtils.min(logStartOffsets, 0L)
  }

  /**
   * 尝试完成任何未决的请求。 这应该被调用，而不需要保存leaderIsrUpdateLock。
   */
  private def tryCompleteDelayedRequests() {
    val requestKey = new TopicPartitionOperationKey(topicPartition)
    replicaManager.tryCompleteDelayedFetch(requestKey)
    replicaManager.tryCompleteDelayedProduce(requestKey)
    replicaManager.tryCompleteDelayedDeleteRecords(requestKey)
  }

  // 可能会收缩isr
  def maybeShrinkIsr(replicaMaxLagTimeMs: Long) {
    val leaderHWIncremented = inWriteLock(leaderIsrUpdateLock) {  // 领导者hw是否更新
      leaderReplicaIfLocal match {// 只处理本地是领导者的分区
        case Some(leaderReplica) =>
          val outOfSyncReplicas = getOutOfSyncReplicas(leaderReplica, replicaMaxLagTimeMs) // 不在同步队列的
          if(outOfSyncReplicas.nonEmpty) {
            val newInSyncReplicas = inSyncReplicas -- outOfSyncReplicas
            assert(newInSyncReplicas.nonEmpty)
            info("Shrinking ISR from %s to %s".format(inSyncReplicas.map(_.brokerId).mkString(","),
              newInSyncReplicas.map(_.brokerId).mkString(",")))
            // 更新zk和缓存中的ISR
            updateIsr(newInSyncReplicas)
            // we may need to increment high watermark since ISR could be down to 1

            replicaManager.isrShrinkRate.mark()
            maybeIncrementLeaderHW(leaderReplica)  // 可能增加hw
          } else {
            false
          }

        case None => false // 如果不再是领导者，什么也不做
      }
    }

    // 一些延迟的操作可能会在更换HW后解除封锁,因为有了可用稳定的数据
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()
  }

  // 获取不在同步队列的副本
  def getOutOfSyncReplicas(leaderReplica: Replica, maxLagMs: Long): Set[Replica] = {
    /**
      * 有两种情况将在这里处理 -
      * 1.卡住追随者：如果副本的狮子座没有更新maxLagMs毫秒，追随者卡住了，应该从ISR中删除
      * 2.慢跟随者：如果复制品在最后一个maxLagMs毫秒内没有读到leo，那么跟随者就会滞后，应该从ISR中删除
      * 这两种情况都是通过检查lastCaughtUpTimeMs来处理的，这代表了最后一次副本完全追上的时间。
      * 如果违反上述任一条件，则认为该副本不同步
     **/
    val candidateReplicas = inSyncReplicas - leaderReplica // 候选分区
    // 滞后的副本
    val laggingReplicas = candidateReplicas.filter(r => (time.milliseconds - r.lastCaughtUpTimeMs) > maxLagMs)
    if (laggingReplicas.nonEmpty)
      debug("Lagging replicas are %s".format(laggingReplicas.map(_.brokerId).mkString(",")))

    laggingReplicas
  }

  def appendRecordsToFutureReplica(records: MemoryRecords) {
    getReplica(Request.FutureLocalReplicaId).get.log.get.appendAsFollower(records)
  }

  // 将记录添加到跟随者
  def appendRecordsToFollower(records: MemoryRecords) {
    // The read lock is needed to prevent the follower replica from being updated while ReplicaAlterDirThread
    // is executing maybeDeleteAndSwapFutureReplica() to replace follower replica with the future replica.
    // 需要读取锁定以防止在ReplicaAlterDirThread执行mayDeleteAndSwapFutureReplica()时更新跟随者副本，以便用随后的副本替换跟随者副本。
    inReadLock(leaderIsrUpdateLock) {
      getReplica().get.log.get.appendAsFollower(records)
    }
  }

  // 将记录添加到领导者
  def appendRecordsToLeader(records: MemoryRecords, isFromClient: Boolean, requiredAcks: Int = 0): LogAppendInfo = {
    val (info, leaderHWIncremented) = inReadLock(leaderIsrUpdateLock) {
      leaderReplicaIfLocal match {
        case Some(leaderReplica) =>
          val log = leaderReplica.log.get
          val minIsr = log.config.minInSyncReplicas
          val inSyncSize = inSyncReplicas.size

          // 如果没有足够的虚拟副本以确保安全，请避免写入领导者
          if (inSyncSize < minIsr && requiredAcks == -1) {
            throw new NotEnoughReplicasException("Number of insync replicas for partition %s is [%d], below required minimum [%d]"
              .format(topicPartition, inSyncSize, minIsr))
          }

          val info = log.appendAsLeader(records, leaderEpoch = this.leaderEpoch, isFromClient)
          // 可能会取消阻止某些追随者获取请求，因为日志结束偏移量已更新
          replicaManager.tryCompleteDelayedFetch(TopicPartitionOperationKey(this.topic, this.partitionId))
          // 我们可能需要增加高水印，因为ISR可能会降至1
          (info, maybeIncrementLeaderHW(leaderReplica))

        case None =>
          throw new NotLeaderForPartitionException("Leader not local for partition %s on broker %d"
            .format(topicPartition, localBrokerId))
      }
    }

    // some delayed operations may be unblocked after HW changed
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()

    info
  }

  def logStartOffset: Long = {
    inReadLock(leaderIsrUpdateLock) {
      leaderReplicaIfLocal.map(_.log.get.logStartOffset).getOrElse(-1)
    }
  }

  /**
   * Update logStartOffset and low watermark if 1) offset <= highWatermark and 2) it is the leader replica.
   * This function can trigger log segment deletion and log rolling.
   *
   * Return low watermark of the partition.
   */
  def deleteRecordsOnLeader(offset: Long): Long = {
    inReadLock(leaderIsrUpdateLock) {
      leaderReplicaIfLocal match {
        case Some(leaderReplica) =>
          if (!leaderReplica.log.get.config.delete)
            throw new PolicyViolationException("Records of partition %s can not be deleted due to the configured policy".format(topicPartition))
          leaderReplica.maybeIncrementLogStartOffset(offset)
          lowWatermarkIfLeader
        case None =>
          throw new NotLeaderForPartitionException("Leader not local for partition %s on broker %d"
            .format(topicPartition, localBrokerId))
      }
    }
  }

  /**
    * 将此分区的本地日志截断到指定偏移量和检查点，以恢复到此偏移量。
    *
    * @param offset 用于截断的偏移量
    * @param isFuture 应该在这个分区的未来日志上执行截断。
    */
  def truncateTo(offset: Long, isFuture: Boolean) {
    // The read lock is needed to prevent the follower replica from being truncated while ReplicaAlterDirThread
    // is executing maybeDeleteAndSwapFutureReplica() to replace follower replica with the future replica.
    inReadLock(leaderIsrUpdateLock) {
      logManager.truncateTo(Map(topicPartition -> offset), isFuture = isFuture)
    }
  }

  /**
    * 删除此分区的本地日志中的所有数据，并在新偏移量处启动日志
    *
    * @param newOffset The new offset to start the log with
    * @param isFuture True iff the truncation should be performed on the future log of this partition
    */
  def truncateFullyAndStartAt(newOffset: Long, isFuture: Boolean) {
    // The read lock is needed to prevent the follower replica from being truncated while ReplicaAlterDirThread
    // is executing maybeDeleteAndSwapFutureReplica() to replace follower replica with the future replica.
    inReadLock(leaderIsrUpdateLock) {
      logManager.truncateFullyAndStartAt(topicPartition, newOffset, isFuture = isFuture)
    }
  }

  /**
    * @param leaderEpoch Requested leader epoch
    * @return 在这个领导者时代发布的消息的最后偏移量。
    */
  def lastOffsetForLeaderEpoch(leaderEpoch: Int): EpochEndOffset = {
    inReadLock(leaderIsrUpdateLock) {
      leaderReplicaIfLocal match {
        case Some(leaderReplica) =>
          new EpochEndOffset(NONE, leaderReplica.epochs.get.endOffsetFor(leaderEpoch))
        case None =>
          new EpochEndOffset(NOT_LEADER_FOR_PARTITION, UNDEFINED_EPOCH_OFFSET)
      }
    }
  }

  private def updateIsr(newIsr: Set[Replica]) {
    val newLeaderAndIsr = new LeaderAndIsr(localBrokerId, leaderEpoch, newIsr.map(_.brokerId).toList, zkVersion)
    // 更新zk
    val (updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(zkClient, topicPartition, newLeaderAndIsr,
      controllerEpoch)

    if (updateSucceeded) { // 更新ISR和zk版本
      replicaManager.recordIsrChange(topicPartition)
      inSyncReplicas = newIsr
      zkVersion = newVersion
      trace("ISR updated to [%s] and zkVersion updated to [%d]".format(newIsr.mkString(","), zkVersion))
    } else {
      replicaManager.failedIsrUpdatesRate.mark()
      info("Cached zkVersion [%d] not equal to that in zookeeper, skip updating ISR".format(zkVersion))
    }
  }

  /**
   * remove deleted log metrics
   */
  def removePartitionMetrics() {
    removeMetric("UnderReplicated", tags)
    removeMetric("UnderMinIsr", tags)
    removeMetric("InSyncReplicasCount", tags)
    removeMetric("ReplicasCount", tags)
    removeMetric("LastStableOffsetLag", tags)
  }

  override def equals(that: Any): Boolean = that match {
    case other: Partition => partitionId == other.partitionId && topic == other.topic && isOffline == other.isOffline
    case _ => false
  }

  override def hashCode: Int =
    31 + topic.hashCode + 17 * partitionId + (if (isOffline) 1 else 0)

  override def toString(): String = {
    val partitionString = new StringBuilder
    partitionString.append("Topic: " + topic)
    partitionString.append("; Partition: " + partitionId)
    partitionString.append("; Leader: " + leaderReplicaIdOpt)
    partitionString.append("; AllReplicas: " + allReplicasMap.keys.mkString(","))
    partitionString.append("; InSyncReplicas: " + inSyncReplicas.map(_.brokerId).mkString(","))
    partitionString.toString
  }
}
