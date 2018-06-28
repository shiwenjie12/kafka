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

import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantReadWriteLock

import kafka.common.KafkaException
import kafka.log.LogConfig
import kafka.message.UncompressedCodec
import kafka.server.Defaults
import kafka.server.ReplicaManager
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.utils.{Logging, Pool, Scheduler}
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{FileRecords, MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests.IsolationLevel
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests.TransactionResult
import org.apache.kafka.common.utils.{Time, Utils}

import scala.collection.mutable
import scala.collection.JavaConverters._


object TransactionStateManager {
  // default transaction management config values
  val DefaultTransactionsMaxTimeoutMs: Int = TimeUnit.MINUTES.toMillis(15).toInt
  val DefaultTransactionalIdExpirationMs: Int = TimeUnit.DAYS.toMillis(7).toInt
  val DefaultAbortTimedOutTransactionsIntervalMs: Int = TimeUnit.MINUTES.toMillis(1).toInt
  val DefaultRemoveExpiredTransactionalIdsIntervalMs: Int = TimeUnit.HOURS.toMillis(1).toInt
}

/**
  * 事务状态管理器是事务协调器的一部分，它管理着：
  *
  * 1.事务日志，这是一个特殊的内部主题。
  * 2.事务元数据包括其正在进行的交易状态。
  * 3.事务的后台到期以及交易ID。
  *
  * <b>Delayed operation locking notes:</b>
  * Delayed operations in TransactionStateManager use `stateLock.readLock` as the delayed operation
  * lock. Delayed operations are completed only if `stateLock.readLock` can be acquired.
  * Delayed callbacks may acquire `stateLock.readLock` or any of the `txnMetadata` locks.
  * <ul>
  * <li>`stateLock.readLock` must never be acquired while holding `txnMetadata` lock.</li>
  * <li>`txnMetadata` lock must never be acquired while holding `stateLock.writeLock`.</li>
  * <li>`ReplicaManager.appendRecords` should never be invoked while holding a `txnMetadata` lock.</li>
  * </ul>
  */
class TransactionStateManager(brokerId: Int,
                              zkClient: KafkaZkClient,
                              scheduler: Scheduler,
                              replicaManager: ReplicaManager,
                              config: TransactionConfig,
                              time: Time) extends Logging {

  this.logIdent = "[Transaction State Manager " + brokerId + "]: "

  type SendTxnMarkersCallback = (String, Int, TransactionResult, TransactionMetadata, TxnTransitMetadata) => Unit

  /** 关闭标识 */
  private val shuttingDown = new AtomicBoolean(false)

  /** 锁定保护对事务性元数据缓存的访问，包括加载和离开分区集 */
  private val stateLock = new ReentrantReadWriteLock()

  /** 正在加载的事务主题的分区，应该在访问此集之前调用状态锁 */
  private val loadingPartitions: mutable.Set[TransactionPartitionAndLeaderEpoch] = mutable.Set()

  /** 正在被删除的事务主题的分区，应该在访问这个集之前调用状态锁 */
  private val leavingPartitions: mutable.Set[TransactionPartitionAndLeaderEpoch] = mutable.Set()

  /** 事务元数据缓存由分配的事务主题分区ID索引 */
  private val transactionMetadataCache: mutable.Map[Int, TxnMetadataCacheEntry] = mutable.Map()

  /** 事务日志主题的分区数 */
  private val transactionTopicPartitionCount = getTransactionTopicPartitionCount

  // visible for testing only
  private[transaction] def addLoadingPartition(partitionId: Int, coordinatorEpoch: Int): Unit = {
    val partitionAndLeaderEpoch = TransactionPartitionAndLeaderEpoch(partitionId, coordinatorEpoch)

    inWriteLock(stateLock) {
      leavingPartitions.remove(partitionAndLeaderEpoch)
      loadingPartitions.add(partitionAndLeaderEpoch)
    }
  }

  private[transaction] def stateReadLock = stateLock.readLock

  // this is best-effort expiration of an ongoing transaction which has been open for more than its
  // txn timeout value, we do not need to grab the lock on the metadata object upon checking its state
  // since the timestamp is volatile and we will get the lock when actually trying to transit the transaction
  // metadata to abort later.
  // 返回过期的事务
  def timedOutTransactions(): Iterable[TransactionalIdAndProducerIdEpoch] = {
    val now = time.milliseconds()
    inReadLock(stateLock) {
      transactionMetadataCache.filter { case (txnPartitionId, _) =>
        !leavingPartitions.exists(_.txnPartitionId == txnPartitionId)
      }.flatMap { case (_, entry) =>
        entry.metadataPerTransactionalId.filter { case (_, txnMetadata) =>
          if (txnMetadata.pendingTransitionInProgress) {
            false
          } else {
            txnMetadata.state match { // 正在进行的，过期的
              case Ongoing =>
                txnMetadata.txnStartTimestamp + txnMetadata.txnTimeoutMs < now
              case _ => false
            }
          }
        }.map { case (txnId, txnMetadata) =>
          TransactionalIdAndProducerIdEpoch(txnId, txnMetadata.producerId, txnMetadata.producerEpoch)
        }
      }
    }
  }

  // 启用事务过期清除
  def enableTransactionalIdExpiration() {
    scheduler.schedule("transactionalId-expiration", () => {
      val now = time.milliseconds()
      inReadLock(stateLock) {
        val transactionalIdByPartition: Map[Int, mutable.Iterable[TransactionalIdCoordinatorEpochAndMetadata]] =
          transactionMetadataCache.flatMap { case (partition, entry) =>
            entry.metadataPerTransactionalId.filter { case (_, txnMetadata) => txnMetadata.state match { // 完成状态和空状态
              case Empty | CompleteCommit | CompleteAbort => true
              case _ => false
            }
            }.filter { case (_, txnMetadata) => // 过期时间
              txnMetadata.txnLastUpdateTimestamp <= now - config.transactionalIdExpirationMs
            }.map { case (transactionalId, txnMetadata) =>
              val txnMetadataTransition = txnMetadata.inLock {
                txnMetadata.prepareDead()
              }
              TransactionalIdCoordinatorEpochAndMetadata(transactionalId, entry.coordinatorEpoch, txnMetadataTransition)
            }
          }.groupBy { transactionalIdCoordinatorEpochAndMetadata =>
            partitionFor(transactionalIdCoordinatorEpochAndMetadata.transactionalId)
          }

        val recordsPerPartition = transactionalIdByPartition
          .map { case (partition, transactionalIdCoordinatorEpochAndMetadatas) =>
            val deletes: Array[SimpleRecord] = transactionalIdCoordinatorEpochAndMetadatas.map { entry =>  // 要删除的记录
              new SimpleRecord(now, TransactionLog.keyToBytes(entry.transactionalId), null)
            }.toArray
            val records = MemoryRecords.withRecords(TransactionLog.EnforcedCompressionType, deletes: _*)
            val topicPartition = new TopicPartition(Topic.TRANSACTION_STATE_TOPIC_NAME, partition)  // 事务分区
            (topicPartition, records)
          }


        // 删除回调
        def removeFromCacheCallback(responses: collection.Map[TopicPartition, PartitionResponse]): Unit = {
          responses.foreach { case (topicPartition, response) =>
            response.error match {
              case Errors.NONE =>
                inReadLock(stateLock) {
                  val toRemove = transactionalIdByPartition(topicPartition.partition)
                  transactionMetadataCache.get(topicPartition.partition) // 移除缓存数据
                    .foreach { txnMetadataCacheEntry =>
                      toRemove.foreach { idCoordinatorEpochAndMetadata =>
                        val txnMetadata = txnMetadataCacheEntry.metadataPerTransactionalId.get(idCoordinatorEpochAndMetadata.transactionalId)
                        txnMetadata.inLock {
                          if (txnMetadataCacheEntry.coordinatorEpoch == idCoordinatorEpochAndMetadata.coordinatorEpoch
                              && txnMetadata.pendingState.contains(Dead)
                              && txnMetadata.producerEpoch == idCoordinatorEpochAndMetadata.transitMetadata.producerEpoch
                            )
                            txnMetadataCacheEntry.metadataPerTransactionalId.remove(idCoordinatorEpochAndMetadata.transactionalId) // 移除死亡事务
                          else {
                            debug(s"failed to remove expired transactionalId: ${idCoordinatorEpochAndMetadata.transactionalId}" +
                              s" from cache. pendingState: ${txnMetadata.pendingState} producerEpoch: ${txnMetadata.producerEpoch}" +
                              s" expected producerEpoch: ${idCoordinatorEpochAndMetadata.transitMetadata.producerEpoch}" +
                              s" coordinatorEpoch: ${txnMetadataCacheEntry.coordinatorEpoch} expected coordinatorEpoch: " +
                              s"${idCoordinatorEpochAndMetadata.coordinatorEpoch}")
                            txnMetadata.pendingState = None
                          }
                        }
                      }
                    }
                }
              case _ =>
                debug(s"writing transactionalId tombstones for partition: ${topicPartition.partition} failed with error: ${response.error.message()}")
            }
          }
        }

        replicaManager.appendRecords(
          config.requestTimeoutMs,
          TransactionLog.EnforcedRequiredAcks,
          internalTopicsAllowed = true,
          isFromClient = false,
          recordsPerPartition,  // 添加删除记录
          removeFromCacheCallback,
          Some(stateLock.readLock)
        )
      }

    }, delay = config.removeExpiredTransactionalIdsIntervalMs, period = config.removeExpiredTransactionalIdsIntervalMs)
  }

  def getTransactionState(transactionalId: String): Either[Errors, Option[CoordinatorEpochAndTxnMetadata]] =
    getAndMaybeAddTransactionState(transactionalId, None)

  // 添加新的事务元素
  def putTransactionStateIfNotExists(transactionalId: String,
                                     txnMetadata: TransactionMetadata): Either[Errors, CoordinatorEpochAndTxnMetadata] =
    getAndMaybeAddTransactionState(transactionalId, Some(txnMetadata))
      .right.map(_.getOrElse(throw new IllegalStateException(s"Unexpected empty transaction metadata returned while putting $txnMetadata")))

  /**
    * 获取与给定事务ID相关联的事务元数据，或者如果协调器不拥有事务分区或仍在加载事务分区，则返回错误;
    * 如果未找到，则返回None或创建新的元数据并添加到缓存中
    * 该功能由stateLock锁定
    */
  private def getAndMaybeAddTransactionState(transactionalId: String,
                                             createdTxnMetadataOpt: Option[TransactionMetadata]): Either[Errors, Option[CoordinatorEpochAndTxnMetadata]] = {
    inReadLock(stateLock) {
      val partitionId = partitionFor(transactionalId)
      if (loadingPartitions.exists(_.txnPartitionId == partitionId))
        Left(Errors.COORDINATOR_LOAD_IN_PROGRESS)
      else if (leavingPartitions.exists(_.txnPartitionId == partitionId))
        Left(Errors.NOT_COORDINATOR)
      else {
        transactionMetadataCache.get(partitionId) match { // 事务分区
          case Some(cacheEntry) =>
            val txnMetadata = Option(cacheEntry.metadataPerTransactionalId.get(transactionalId)).orElse { // 事务id拥有的元数据
              createdTxnMetadataOpt.map { createdTxnMetadata => // 缓存事务元数据
                Option(cacheEntry.metadataPerTransactionalId.putIfNotExists(transactionalId, createdTxnMetadata))
                  .getOrElse(createdTxnMetadata)
              }
            }
            Right(txnMetadata.map(CoordinatorEpochAndTxnMetadata(cacheEntry.coordinatorEpoch, _)))

          case None =>
            Left(Errors.NOT_COORDINATOR)
        }
      }
    }
  }

  /**
    * 验证给定的事务超时值
    */
  def validateTransactionTimeoutMs(txnTimeoutMs: Int): Boolean =
    txnTimeoutMs <= config.transactionMaxTimeoutMs && txnTimeoutMs > 0

  def transactionTopicConfigs: Properties = {
    val props = new Properties

    // enforce disabled unclean leader election, no compression types, and compact cleanup policy
    props.put(LogConfig.UncleanLeaderElectionEnableProp, "false")
    props.put(LogConfig.CompressionTypeProp, UncompressedCodec.name)
    props.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)
    props.put(LogConfig.MinInSyncReplicasProp, config.transactionLogMinInsyncReplicas.toString)
    props.put(LogConfig.SegmentBytesProp, config.transactionLogSegmentBytes.toString)

    props
  }

  def partitionFor(transactionalId: String): Int = Utils.abs(transactionalId.hashCode) % transactionTopicPartitionCount

  /**
    * 从ZooKeeper获取事务日志主题的分区计数。
    * 如果该主题不存在，则返回默认分区计数。
    */
  private def getTransactionTopicPartitionCount: Int = {
    zkClient.getTopicPartitionCount(Topic.TRANSACTION_STATE_TOPIC_NAME).getOrElse(config.transactionLogNumPartitions)
  }

  private def loadTransactionMetadata(topicPartition: TopicPartition, coordinatorEpoch: Int): Pool[String, TransactionMetadata] = {
    def logEndOffset = replicaManager.getLogEndOffset(topicPartition).getOrElse(-1L)

    val startMs = time.milliseconds()
    val loadedTransactions = new Pool[String, TransactionMetadata]

    replicaManager.getLog(topicPartition) match {
      case None =>
        warn(s"Attempted to load offsets and group metadata from $topicPartition, but found no log")

      case Some(log) =>
        lazy val buffer = ByteBuffer.allocate(config.transactionLogLoadBufferSize)

        // loop breaks if leader changes at any time during the load, since getHighWatermark is -1
        var currOffset = log.logStartOffset

        try {
          while (currOffset < logEndOffset
            && !shuttingDown.get()
            && inReadLock(stateLock) {
            loadingPartitions.exists { idAndEpoch: TransactionPartitionAndLeaderEpoch =>
              idAndEpoch.txnPartitionId == topicPartition.partition && idAndEpoch.coordinatorEpoch == coordinatorEpoch
            }
          }) {
            val fetchDataInfo = log.read(currOffset, config.transactionLogLoadBufferSize, maxOffset = None,
              minOneMessage = true, isolationLevel = IsolationLevel.READ_UNCOMMITTED)
            val memRecords = fetchDataInfo.records match {
              case records: MemoryRecords => records
              case fileRecords: FileRecords =>
                buffer.clear()
                val bufferRead = fileRecords.readInto(buffer, 0)
                MemoryRecords.readableRecords(bufferRead)
            }

            memRecords.batches.asScala.foreach { batch =>
              for (record <- batch.asScala) {
                require(record.hasKey, "Transaction state log's key should not be null")
                val txnKey = TransactionLog.readTxnRecordKey(record.key)
                // load transaction metadata along with transaction state
                val transactionalId = txnKey.transactionalId
                if (!record.hasValue) {
                  loadedTransactions.remove(transactionalId)
                } else {
                  val txnMetadata = TransactionLog.readTxnRecordValue(transactionalId, record.value)
                  loadedTransactions.put(transactionalId, txnMetadata)
                }
                currOffset = batch.nextOffset
              }
            }

            info(s"Finished loading ${loadedTransactions.size} transaction metadata from $topicPartition in ${time.milliseconds() - startMs} milliseconds")
          }
        } catch {
          case t: Throwable => error(s"Error loading transactions from transaction log $topicPartition", t)
        }
    }

    loadedTransactions
  }

  /**
    * Add a transaction topic partition into the cache
    *
    * Make it package-private to be used only for unit tests.
    */
  private[transaction] def addLoadedTransactionsToCache(txnTopicPartition: Int, coordinatorEpoch: Int, metadataPerTransactionalId: Pool[String, TransactionMetadata]): Unit = {
    val txnMetadataCacheEntry = TxnMetadataCacheEntry(coordinatorEpoch, metadataPerTransactionalId)
    val currentTxnMetadataCacheEntry = transactionMetadataCache.put(txnTopicPartition, txnMetadataCacheEntry)

    if (currentTxnMetadataCacheEntry.isDefined) {
      val coordinatorEpoch = currentTxnMetadataCacheEntry.get.coordinatorEpoch
      val metadataPerTxnId = currentTxnMetadataCacheEntry.get.metadataPerTransactionalId
      val errorMsg = s"The metadata cache for txn partition $txnTopicPartition has already exist with epoch $coordinatorEpoch " +
        s"and ${metadataPerTxnId.size} entries while trying to add to it; " +
        s"this should not happen"
      fatal(errorMsg)
      throw new IllegalStateException(errorMsg)
    }
  }

  /**
    * When this broker becomes a leader for a transaction log partition, load this partition and
    * populate the transaction metadata cache with the transactional ids.
    */
  def loadTransactionsForTxnTopicPartition(partitionId: Int, coordinatorEpoch: Int, sendTxnMarkers: SendTxnMarkersCallback) {
    validateTransactionTopicPartitionCountIsStable()

    val topicPartition = new TopicPartition(Topic.TRANSACTION_STATE_TOPIC_NAME, partitionId)
    val partitionAndLeaderEpoch = TransactionPartitionAndLeaderEpoch(partitionId, coordinatorEpoch)

    inWriteLock(stateLock) {
      leavingPartitions.remove(partitionAndLeaderEpoch)
      loadingPartitions.add(partitionAndLeaderEpoch)
    }

    def loadTransactions() {
      info(s"Loading transaction metadata from $topicPartition")
      val loadedTransactions = loadTransactionMetadata(topicPartition, coordinatorEpoch)

      inWriteLock(stateLock) {
        if (loadingPartitions.contains(partitionAndLeaderEpoch)) {
          addLoadedTransactionsToCache(topicPartition.partition, coordinatorEpoch, loadedTransactions)

          val transactionsPendingForCompletion = new mutable.ListBuffer[TransactionalIdCoordinatorEpochAndTransitMetadata]
          loadedTransactions.foreach {
            case (transactionalId, txnMetadata) =>
              txnMetadata.inLock {
                // if state is PrepareCommit or PrepareAbort we need to complete the transaction
                txnMetadata.state match {
                  case PrepareAbort =>
                    transactionsPendingForCompletion +=
                      TransactionalIdCoordinatorEpochAndTransitMetadata(transactionalId, coordinatorEpoch, TransactionResult.ABORT, txnMetadata, txnMetadata.prepareComplete(time.milliseconds()))
                  case PrepareCommit =>
                    transactionsPendingForCompletion +=
                      TransactionalIdCoordinatorEpochAndTransitMetadata(transactionalId, coordinatorEpoch, TransactionResult.COMMIT, txnMetadata, txnMetadata.prepareComplete(time.milliseconds()))
                  case _ =>
                  // nothing need to be done
                }
              }
          }

          // we first remove the partition from loading partition then send out the markers for those pending to be
          // completed transactions, so that when the markers get sent the attempt of appending the complete transaction
          // log would not be blocked by the coordinator loading error
          loadingPartitions.remove(partitionAndLeaderEpoch)

          transactionsPendingForCompletion.foreach { txnTransitMetadata =>
            sendTxnMarkers(txnTransitMetadata.transactionalId, txnTransitMetadata.coordinatorEpoch, txnTransitMetadata.result, txnTransitMetadata.txnMetadata, txnTransitMetadata.transitMetadata)
          }
        }
      }
    }

    scheduler.schedule(s"load-txns-for-partition-$topicPartition", loadTransactions)
  }

  /**
    * When this broker becomes a follower for a transaction log partition, clear out the cache for corresponding transactional ids
    * that belong to that partition.
    */
  def removeTransactionsForTxnTopicPartition(partitionId: Int, coordinatorEpoch: Int) {
    validateTransactionTopicPartitionCountIsStable()

    val topicPartition = new TopicPartition(Topic.TRANSACTION_STATE_TOPIC_NAME, partitionId)
    val partitionAndLeaderEpoch = TransactionPartitionAndLeaderEpoch(partitionId, coordinatorEpoch)

    inWriteLock(stateLock) {
      loadingPartitions.remove(partitionAndLeaderEpoch)
      leavingPartitions.add(partitionAndLeaderEpoch)
    }

    def removeTransactions() {
      inWriteLock(stateLock) {
        if (leavingPartitions.contains(partitionAndLeaderEpoch)) {
          transactionMetadataCache.remove(partitionId) match {
            case Some(txnMetadataCacheEntry) =>
              info(s"Removed ${txnMetadataCacheEntry.metadataPerTransactionalId.size} cached transaction metadata for $topicPartition on follower transition")

            case None =>
              info(s"Trying to remove cached transaction metadata for $topicPartition on follower transition but there is no entries remaining; " +
                s"it is likely that another process for removing the cached entries has just executed earlier before")
          }

          leavingPartitions.remove(partitionAndLeaderEpoch)
        }
      }
    }

    scheduler.schedule(s"remove-txns-for-partition-$topicPartition", removeTransactions)
  }

  private def validateTransactionTopicPartitionCountIsStable(): Unit = {
    val curTransactionTopicPartitionCount = getTransactionTopicPartitionCount
    if (transactionTopicPartitionCount != curTransactionTopicPartitionCount)
      throw new KafkaException(s"Transaction topic number of partitions has changed from $transactionTopicPartitionCount to $curTransactionTopicPartitionCount")
  }

  def appendTransactionToLog(transactionalId: String,
                             coordinatorEpoch: Int,
                             newMetadata: TxnTransitMetadata,
                             responseCallback: Errors => Unit,
                             retryOnError: Errors => Boolean = _ => false): Unit = {

    // 为此事务元数据生成消息
    val keyBytes = TransactionLog.keyToBytes(transactionalId)
    val valueBytes = TransactionLog.valueToBytes(newMetadata)
    val timestamp = time.milliseconds()

    val records = MemoryRecords.withRecords(TransactionLog.EnforcedCompressionType, new SimpleRecord(timestamp, keyBytes, valueBytes))
    val topicPartition = new TopicPartition(Topic.TRANSACTION_STATE_TOPIC_NAME, partitionFor(transactionalId))
    val recordsPerPartition = Map(topicPartition -> records)

    // 设置回调函数以在日志追加完成后更新缓存中的事务状态
    def updateCacheCallback(responseStatus: collection.Map[TopicPartition, PartitionResponse]): Unit = {
      // the append response should only contain the topics partition
      if (responseStatus.size != 1 || !responseStatus.contains(topicPartition))
        throw new IllegalStateException("Append status %s should only have one partition %s"
          .format(responseStatus, topicPartition))

      val status = responseStatus(topicPartition)

      var responseError = if (status.error == Errors.NONE) {
        Errors.NONE
      } else {
        debug(s"Appending $transactionalId's new metadata $newMetadata failed due to ${status.error.exceptionName}")

        // 将日志附加错误代码转换为相应的协调器错误代码
        status.error match {
          case Errors.UNKNOWN_TOPIC_OR_PARTITION
               | Errors.NOT_ENOUGH_REPLICAS
               | Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND
               | Errors.REQUEST_TIMED_OUT => // note that for timed out request we return NOT_AVAILABLE error code to let client retry
            Errors.COORDINATOR_NOT_AVAILABLE

          case Errors.NOT_LEADER_FOR_PARTITION
               | Errors.KAFKA_STORAGE_ERROR =>
            Errors.NOT_COORDINATOR

          case Errors.MESSAGE_TOO_LARGE
               | Errors.RECORD_LIST_TOO_LARGE =>
            Errors.UNKNOWN_SERVER_ERROR

          case other =>
            other
        }
      }

      if (responseError == Errors.NONE) {
        // now try to update the cache: we need to update the status in-place instead of
        // overwriting the whole object to ensure synchronization
        getTransactionState(transactionalId) match {

          case Left(err) =>
            info(s"Accessing the cached transaction metadata for $transactionalId returns $err error; " +
              s"aborting transition to the new metadata and setting the error in the callback")
            responseError = err
          case Right(Some(epochAndMetadata)) =>
            val metadata = epochAndMetadata.transactionMetadata

            metadata.inLock {
              if (epochAndMetadata.coordinatorEpoch != coordinatorEpoch) {
                // the cache may have been changed due to txn topic partition emigration and immigration,
                // in this case directly return NOT_COORDINATOR to client and let it to re-discover the transaction coordinator
                info(s"The cached coordinator epoch for $transactionalId has changed to ${epochAndMetadata.coordinatorEpoch} after appended its new metadata $newMetadata " +
                  s"to the transaction log (txn topic partition ${partitionFor(transactionalId)}) while it was $coordinatorEpoch before appending; " +
                  s"aborting transition to the new metadata and returning ${Errors.NOT_COORDINATOR} in the callback")
                responseError = Errors.NOT_COORDINATOR
              } else {
                metadata.completeTransitionTo(newMetadata) // 完成事务
                debug(s"Updating $transactionalId's transaction state to $newMetadata with coordinator epoch $coordinatorEpoch for $transactionalId succeeded")
              }
            }

          case Right(None) =>
            // this transactional id no longer exists, maybe the corresponding partition has already been migrated out.
            // return NOT_COORDINATOR to let the client re-discover the transaction coordinator
            info(s"The cached coordinator metadata does not exist in the cache anymore for $transactionalId after appended its new metadata $newMetadata " +
              s"to the transaction log (txn topic partition ${partitionFor(transactionalId)}) while it was $coordinatorEpoch before appending; " +
              s"aborting transition to the new metadata and returning ${Errors.NOT_COORDINATOR} in the callback")
            responseError = Errors.NOT_COORDINATOR
        }
      } else { // 有错误的
        // 返回错误时重置挂起状态，因为此时没有事务性标识的活动事务。
        getTransactionState(transactionalId) match {
          case Right(Some(epochAndTxnMetadata)) =>
            val metadata = epochAndTxnMetadata.transactionMetadata
            metadata.inLock {
              if (epochAndTxnMetadata.coordinatorEpoch == coordinatorEpoch) {
                if (retryOnError(responseError)) {
                  info(s"TransactionalId ${metadata.transactionalId} append transaction log for $newMetadata transition failed due to $responseError, " +
                    s"not resetting pending state ${metadata.pendingState} but just returning the error in the callback to let the caller retry")
                } else {
                  info(s"TransactionalId ${metadata.transactionalId} append transaction log for $newMetadata transition failed due to $responseError, " +
                    s"resetting pending state from ${metadata.pendingState}, aborting state transition and returning $responseError in the callback")

                  metadata.pendingState = None
                }
              } else {
                info(s"TransactionalId ${metadata.transactionalId} append transaction log for $newMetadata transition failed due to $responseError, " +
                  s"aborting state transition and returning the error in the callback since the coordinator epoch has changed from ${epochAndTxnMetadata.coordinatorEpoch} to $coordinatorEpoch")
              }
            }

          case Right(None) =>
            // Do nothing here, since we want to return the original append error to the user.
            info(s"TransactionalId $transactionalId append transaction log for $newMetadata transition failed due to $responseError, " +
              s"aborting state transition and returning the error in the callback since metadata is not available in the cache anymore")

          case Left(error) =>
            // Do nothing here, since we want to return the original append error to the user.
            info(s"TransactionalId $transactionalId append transaction log for $newMetadata transition failed due to $responseError, " +
              s"aborting state transition and returning the error in the callback since retrieving metadata returned $error")
        }

      }

      responseCallback(responseError)
    }

    inReadLock(stateLock) {
      // 我们需要在事务元数据缓存中保存读锁定，直到附加到本地日志返回为止;
      // 这是为了避免移植之后的移民可能在检查返回之后并且在调用appendRecords（）之前完成的情况，
      // 因为否则条目 在这两个事件之间的日志中可能会附加一个高协调器时期，因此appendRecords（）会在旧协调器时期追加条目，
      // 这些条件仍然可以在追随者上成功复制并使日志处于不良状态。
      getTransactionState(transactionalId) match {
        case Left(err) =>
          responseCallback(err)

        case Right(None) =>
          // the coordinator metadata has been removed, reply to client immediately with NOT_COORDINATOR
          responseCallback(Errors.NOT_COORDINATOR)

        case Right(Some(epochAndMetadata)) =>
          val metadata = epochAndMetadata.transactionMetadata

          val append: Boolean = metadata.inLock {
            if (epochAndMetadata.coordinatorEpoch != coordinatorEpoch) {
              // 协调员时代发生了变化，立即用NOT_COORDINATOR回复客户端
              responseCallback(Errors.NOT_COORDINATOR)
              false
            } else {
              // 不需要检查元数据对象本身，因为没有并发线程应该能够在相同的协调器时期修改它，所以现在直接附加到txn日志
              true
            }
          }
          if (append) {
            replicaManager.appendRecords(
              newMetadata.txnTimeoutMs.toLong,
              TransactionLog.EnforcedRequiredAcks,
              internalTopicsAllowed = true,
              isFromClient = false,
              recordsPerPartition,
              updateCacheCallback,
              delayedProduceLock = Some(stateLock.readLock))

            trace(s"Appending new metadata $newMetadata for transaction id $transactionalId with coordinator epoch $coordinatorEpoch to the local transaction log")
          }
      }
    }
  }

  def shutdown() {
    shuttingDown.set(true)
    loadingPartitions.clear()
    transactionMetadataCache.clear()

    info("Shutdown complete")
  }
}


private[transaction] case class TxnMetadataCacheEntry(coordinatorEpoch: Int, metadataPerTransactionalId: Pool[String, TransactionMetadata])

private[transaction] case class CoordinatorEpochAndTxnMetadata(coordinatorEpoch: Int, transactionMetadata: TransactionMetadata)

// 事务配置
private[transaction] case class TransactionConfig(transactionalIdExpirationMs: Int = TransactionStateManager.DefaultTransactionalIdExpirationMs,
                                                  transactionMaxTimeoutMs: Int = TransactionStateManager.DefaultTransactionsMaxTimeoutMs,
                                                  transactionLogNumPartitions: Int = TransactionLog.DefaultNumPartitions,
                                                  transactionLogReplicationFactor: Short = TransactionLog.DefaultReplicationFactor,
                                                  transactionLogSegmentBytes: Int = TransactionLog.DefaultSegmentBytes,
                                                  transactionLogLoadBufferSize: Int = TransactionLog.DefaultLoadBufferSize,
                                                  transactionLogMinInsyncReplicas: Int = TransactionLog.DefaultMinInSyncReplicas,
                                                  abortTimedOutTransactionsIntervalMs: Int = TransactionStateManager.DefaultAbortTimedOutTransactionsIntervalMs,
                                                  removeExpiredTransactionalIdsIntervalMs: Int = TransactionStateManager.DefaultRemoveExpiredTransactionalIdsIntervalMs,
                                                  requestTimeoutMs: Int = Defaults.RequestTimeoutMs)

case class TransactionalIdAndProducerIdEpoch(transactionalId: String, producerId: Long, producerEpoch: Short)

case class TransactionPartitionAndLeaderEpoch(txnPartitionId: Int, coordinatorEpoch: Int)

case class TransactionalIdCoordinatorEpochAndMetadata(transactionalId: String, coordinatorEpoch: Int, transitMetadata: TxnTransitMetadata)

case class TransactionalIdCoordinatorEpochAndTransitMetadata(transactionalId: String, coordinatorEpoch: Int, result: TransactionResult, txnMetadata: TransactionMetadata, transitMetadata: TxnTransitMetadata)
