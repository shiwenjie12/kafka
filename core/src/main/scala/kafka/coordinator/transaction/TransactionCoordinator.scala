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

import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import kafka.server.{DelayedOperationPurgatory, KafkaConfig, MetadataCache, ReplicaManager}
import kafka.utils.{Logging, Scheduler}
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.TransactionResult
import org.apache.kafka.common.utils.{LogContext, Time}

object TransactionCoordinator {

  def apply(config: KafkaConfig,
            replicaManager: ReplicaManager,
            scheduler: Scheduler,
            zkClient: KafkaZkClient,
            metrics: Metrics,
            metadataCache: MetadataCache,
            time: Time): TransactionCoordinator = {

    val txnConfig = TransactionConfig(config.transactionalIdExpirationMs,
      config.transactionMaxTimeoutMs,
      config.transactionTopicPartitions,
      config.transactionTopicReplicationFactor,
      config.transactionTopicSegmentBytes,
      config.transactionsLoadBufferSize,
      config.transactionTopicMinISR,
      config.transactionAbortTimedOutTransactionCleanupIntervalMs,
      config.transactionRemoveExpiredTransactionalIdCleanupIntervalMs,
      config.requestTimeoutMs)

    val producerIdManager = new ProducerIdManager(config.brokerId, zkClient)
    // we do not need to turn on reaper thread since no tasks will be expired and there are no completed tasks to be purged
    val txnMarkerPurgatory = DelayedOperationPurgatory[DelayedTxnMarker]("txn-marker-purgatory", config.brokerId,
      reaperEnabled = false, timerEnabled = false)
    val txnStateManager = new TransactionStateManager(config.brokerId, zkClient, scheduler, replicaManager, txnConfig, time)

    val logContext = new LogContext(s"[TransactionCoordinator id=${config.brokerId}] ")
    val txnMarkerChannelManager = TransactionMarkerChannelManager(config, metrics, metadataCache, txnStateManager,
      txnMarkerPurgatory, time, logContext)

    new TransactionCoordinator(config.brokerId, txnConfig, scheduler, producerIdManager, txnStateManager, txnMarkerChannelManager,
      time, logContext)
  }

  private def initTransactionError(error: Errors): InitProducerIdResult = {
    InitProducerIdResult(RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, error)
  }

  private def initTransactionMetadata(txnMetadata: TxnTransitMetadata): InitProducerIdResult = {
    InitProducerIdResult(txnMetadata.producerId, txnMetadata.producerEpoch, Errors.NONE)
  }
}

/**
 * 事务协调员处理由生产者发送的消息交易并与经纪人通信以更新持续交易的状态。
  *
 * 每个Kafka服务器实例化一个负责一组生产者的事务协调器。
  * 具有特定交易ID的生产者被分配给其相应的协调员; 没有具体交易编号的制片人可能会与随机经纪人作为他们的协调员。
 */
class TransactionCoordinator(brokerId: Int,
                             txnConfig: TransactionConfig,
                             scheduler: Scheduler,
                             producerIdManager: ProducerIdManager,
                             txnManager: TransactionStateManager,
                             txnMarkerChannelManager: TransactionMarkerChannelManager,
                             time: Time,
                             logContext: LogContext) extends Logging {
  this.logIdent = logContext.logPrefix

  import TransactionCoordinator._

  type InitProducerIdCallback = InitProducerIdResult => Unit
  type AddPartitionsCallback = Errors => Unit
  type EndTxnCallback = Errors => Unit
  type ApiResult[T] = Either[Errors, T]

  /* Active flag of the coordinator */
  private val isActive = new AtomicBoolean(false)

  // 初始化生产者id
  def handleInitProducerId(transactionalId: String,
                           transactionTimeoutMs: Int,
                           responseCallback: InitProducerIdCallback): Unit = {

    if (transactionalId == null) { // 走不到这里面的逻辑
      // 如果事务标识为空，则始终盲目接受请求并从producerId管理器返回新的producerId
      val producerId = producerIdManager.generateProducerId()
      responseCallback(InitProducerIdResult(producerId, producerEpoch = 0, Errors.NONE))
    } else if (transactionalId.isEmpty) {
      // 如果事务标识为空，则返回错误作为无效请求。 这是为了使TransactionCoordinator的行为与生产者客户端一致
      responseCallback(initTransactionError(Errors.INVALID_REQUEST))
    } else if (!txnManager.validateTransactionTimeoutMs(transactionTimeoutMs)) {
      // 请检查transactionTimeoutMs是否不大于代理配置的最大允许值
      responseCallback(initTransactionError(Errors.INVALID_TRANSACTION_TIMEOUT))
    } else {
      val coordinatorEpochAndMetadata = txnManager.getTransactionState(transactionalId).right.flatMap {
        case None => // 构造新的事务元数据
          val producerId = producerIdManager.generateProducerId() // 生产者id
          val createdMetadata = new TransactionMetadata(transactionalId = transactionalId, // 创建事务元素
            producerId = producerId,
            producerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
            txnTimeoutMs = transactionTimeoutMs,
            state = Empty,
            topicPartitions = collection.mutable.Set.empty[TopicPartition],
            txnLastUpdateTimestamp = time.milliseconds())
          txnManager.putTransactionStateIfNotExists(transactionalId, createdMetadata)

        case Some(epochAndTxnMetadata) => Right(epochAndTxnMetadata)
      }

      val result: ApiResult[(Int, TxnTransitMetadata)] = coordinatorEpochAndMetadata.right.flatMap {
        existingEpochAndMetadata =>
          val coordinatorEpoch = existingEpochAndMetadata.coordinatorEpoch
          val txnMetadata = existingEpochAndMetadata.transactionMetadata

          txnMetadata.inLock {
            prepareInitProduceIdTransit(transactionalId, transactionTimeoutMs, coordinatorEpoch, txnMetadata)
          }
      }

      result match {
        case Left(error) =>
          responseCallback(initTransactionError(error))

        case Right((coordinatorEpoch, newMetadata)) =>
          if (newMetadata.txnState == PrepareEpochFence) {
            // 中止正在进行的事务，然后返回CONCURRENT_TRANSACTIONS以让客户端等待并重试
            def sendRetriableErrorCallback(error: Errors): Unit = {
              if (error != Errors.NONE) {
                responseCallback(initTransactionError(error))
              } else {
                responseCallback(initTransactionError(Errors.CONCURRENT_TRANSACTIONS)) // 中断之前的事务
              }
            }

            handleEndTransaction(transactionalId, // 中断当前事务
              newMetadata.producerId,
              newMetadata.producerEpoch,
              TransactionResult.ABORT,
              sendRetriableErrorCallback)
          } else {
            // 发送pid的回调
            def sendPidResponseCallback(error: Errors): Unit = {
              if (error == Errors.NONE) {
                info(s"Initialized transactionalId $transactionalId with producerId ${newMetadata.producerId} and producer " +
                  s"epoch ${newMetadata.producerEpoch} on partition " +
                  s"${Topic.TRANSACTION_STATE_TOPIC_NAME}-${txnManager.partitionFor(transactionalId)}")
                responseCallback(initTransactionMetadata(newMetadata))
              } else {
                info(s"Returning $error error code to client for $transactionalId's InitProducerId request")
                responseCallback(initTransactionError(error))
              }
            }

            txnManager.appendTransactionToLog(transactionalId, coordinatorEpoch, newMetadata, sendPidResponseCallback) // 初始化生产者id
          }
      }
    }
  }

  // 预初始化
  private def prepareInitProduceIdTransit(transactionalId: String,
                                          transactionTimeoutMs: Int,
                                          coordinatorEpoch: Int,
                                          txnMetadata: TransactionMetadata): ApiResult[(Int, TxnTransitMetadata)] = {
    if (txnMetadata.pendingTransitionInProgress) { // 有正在进行的事务
      // return a retriable exception to let the client backoff and retry
      Left(Errors.CONCURRENT_TRANSACTIONS)
    } else {
      // 调用者应该已经在txnMetadata上同步了
      txnMetadata.state match {
        case PrepareAbort | PrepareCommit =>
          // reply to client and let it backoff and retry
          Left(Errors.CONCURRENT_TRANSACTIONS)

        case CompleteAbort | CompleteCommit | Empty => // 进行初始化
          val transitMetadata = if (txnMetadata.isProducerEpochExhausted) { // epoch枯竭
            val newProducerId = producerIdManager.generateProducerId()
            txnMetadata.prepareProducerIdRotation(newProducerId, transactionTimeoutMs, time.milliseconds())
          } else {
            txnMetadata.prepareIncrementProducerEpoch(transactionTimeoutMs, time.milliseconds())
          }

          Right(coordinatorEpoch, transitMetadata)

        case Ongoing =>
          // 表示首先中止当前正在进行的txn。 请注意，这个epoch永远不会返回给用户。
          // 我们将中止正在进行的交易并将CONCURRENT_TRANSACTIONS退还给客户。
          // 这迫使客户重试，这将确保该时代第二次被碰撞。
          // 特别是，如果屏蔽当前生产者耗尽当前producerId的可用时期，那么当客户重试时，我们将生成新的producerId。
          Right(coordinatorEpoch, txnMetadata.prepareFenceProducerEpoch()) // 终止当前epoch

        case Dead | PrepareEpochFence =>
          val errorMsg = s"Found transactionalId $transactionalId with state ${txnMetadata.state}. " +
            s"This is illegal as we should never have transitioned to this state."
          fatal(errorMsg)
          throw new IllegalStateException(errorMsg)

      }
    }
  }

  def handleAddPartitionsToTransaction(transactionalId: String,
                                       producerId: Long,
                                       producerEpoch: Short,
                                       partitions: collection.Set[TopicPartition],
                                       responseCallback: AddPartitionsCallback): Unit = {
    if (transactionalId == null || transactionalId.isEmpty) {
      debug(s"Returning ${Errors.INVALID_REQUEST} error code to client for $transactionalId's AddPartitions request")
      responseCallback(Errors.INVALID_REQUEST)
    } else {
      // try to update the transaction metadata and append the updated metadata to txn log;
      // if there is no such metadata treat it as invalid producerId mapping error.
      val result: ApiResult[(Int, TxnTransitMetadata)] = txnManager.getTransactionState(transactionalId).right.flatMap {
        case None => Left(Errors.INVALID_PRODUCER_ID_MAPPING)

        case Some(epochAndMetadata) =>
          val coordinatorEpoch = epochAndMetadata.coordinatorEpoch
          val txnMetadata = epochAndMetadata.transactionMetadata

          // 使用添加的分区生成新的事务元数据
          txnMetadata.inLock {
            if (txnMetadata.producerId != producerId) {
              Left(Errors.INVALID_PRODUCER_ID_MAPPING)
            } else if (txnMetadata.producerEpoch != producerEpoch) {
              Left(Errors.INVALID_PRODUCER_EPOCH)
            } else if (txnMetadata.pendingTransitionInProgress) {
              // return a retriable exception to let the client backoff and retry
              Left(Errors.CONCURRENT_TRANSACTIONS)
            } else if (txnMetadata.state == PrepareCommit || txnMetadata.state == PrepareAbort) {
              Left(Errors.CONCURRENT_TRANSACTIONS)
            } else if (txnMetadata.state == Ongoing && partitions.subsetOf(txnMetadata.topicPartitions)) {
              // 这是一个优化：如果分区已经在元数据中，立即回复确定
              Left(Errors.NONE)
            } else {
              Right(coordinatorEpoch, txnMetadata.prepareAddPartitions(partitions.toSet, time.milliseconds()))
            }
          }
      }

      result match {
        case Left(err) =>
          debug(s"Returning $err error code to client for $transactionalId's AddPartitions request")
          responseCallback(err)

        case Right((coordinatorEpoch, newMetadata)) =>
          txnManager.appendTransactionToLog(transactionalId, coordinatorEpoch, newMetadata, responseCallback) // 添加分区到事务中
      }
    }
  }

  def handleTxnImmigration(txnTopicPartitionId: Int, coordinatorEpoch: Int) {
    txnManager.loadTransactionsForTxnTopicPartition(txnTopicPartitionId, coordinatorEpoch, txnMarkerChannelManager.addTxnMarkersToSend)
  }

  def handleTxnEmigration(txnTopicPartitionId: Int, coordinatorEpoch: Int) {
      txnManager.removeTransactionsForTxnTopicPartition(txnTopicPartitionId, coordinatorEpoch)
      txnMarkerChannelManager.removeMarkersForTxnTopicPartition(txnTopicPartitionId)
  }

  private def logInvalidStateTransitionAndReturnError(transactionalId: String,
                                                      transactionState: TransactionState,
                                                      transactionResult: TransactionResult) = {
    debug(s"TransactionalId: $transactionalId's state is $transactionState, but received transaction " +
      s"marker result to send: $transactionResult")
    Left(Errors.INVALID_TXN_STATE)
  }

  // 处理结束事务
  def handleEndTransaction(transactionalId: String,
                           producerId: Long,
                           producerEpoch: Short,
                           txnMarkerResult: TransactionResult,
                           responseCallback: EndTxnCallback): Unit = {
    if (transactionalId == null || transactionalId.isEmpty)
      responseCallback(Errors.INVALID_REQUEST)
    else {
      val preAppendResult: ApiResult[(Int, TxnTransitMetadata)] = txnManager.getTransactionState(transactionalId).right.flatMap {
        case None =>
          Left(Errors.INVALID_PRODUCER_ID_MAPPING)

        case Some(epochAndTxnMetadata) =>
          val txnMetadata = epochAndTxnMetadata.transactionMetadata
          val coordinatorEpoch = epochAndTxnMetadata.coordinatorEpoch

          txnMetadata.inLock {
            if (txnMetadata.producerId != producerId)
              Left(Errors.INVALID_PRODUCER_ID_MAPPING)
            else if (producerEpoch < txnMetadata.producerEpoch)
              Left(Errors.INVALID_PRODUCER_EPOCH)
            else if (txnMetadata.pendingTransitionInProgress && txnMetadata.pendingState.get != PrepareEpochFence)
              Left(Errors.CONCURRENT_TRANSACTIONS)
            else txnMetadata.state match {
              case Ongoing =>
                val nextState = if (txnMarkerResult == TransactionResult.COMMIT) // 接下来的状态
                  PrepareCommit
                else
                  PrepareAbort

                if (nextState == PrepareAbort && txnMetadata.pendingState.contains(PrepareEpochFence)) {
                  // 我们应该清除待处理状态，以便让过渡到PrepareAbort，并且冲刷我们将要追加的事务元数据中的时代。
                  txnMetadata.pendingState = None
                  txnMetadata.producerEpoch = producerEpoch
                }

                Right(coordinatorEpoch, txnMetadata.prepareAbortOrCommit(nextState, time.milliseconds()))  // 开始处理
              case CompleteCommit =>
                if (txnMarkerResult == TransactionResult.COMMIT)
                  Left(Errors.NONE)
                else
                  logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
              case CompleteAbort =>
                if (txnMarkerResult == TransactionResult.ABORT)
                  Left(Errors.NONE)
                else
                  logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
              case PrepareCommit =>
                if (txnMarkerResult == TransactionResult.COMMIT)
                  Left(Errors.CONCURRENT_TRANSACTIONS)
                else
                  logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
              case PrepareAbort =>
                if (txnMarkerResult == TransactionResult.ABORT)
                  Left(Errors.CONCURRENT_TRANSACTIONS)
                else
                  logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
              case Empty =>
                logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
              case Dead | PrepareEpochFence =>
                val errorMsg = s"Found transactionalId $transactionalId with state ${txnMetadata.state}. " +
                  s"This is illegal as we should never have transitioned to this state."
                fatal(errorMsg)
                throw new IllegalStateException(errorMsg)

            }
          }
      }

      preAppendResult match {
        case Left(err) =>
          debug(s"Aborting append of $txnMarkerResult to transaction log with coordinator and returning $err error to client for $transactionalId's EndTransaction request")
          responseCallback(err)

        case Right((coordinatorEpoch, newMetadata)) =>
          def sendTxnMarkersCallback(error: Errors): Unit = {
            if (error == Errors.NONE) {
              val preSendResult: ApiResult[(TransactionMetadata, TxnTransitMetadata)] = txnManager.getTransactionState(transactionalId).right.flatMap {
                case None =>
                  val errorMsg = s"The coordinator still owns the transaction partition for $transactionalId, but there is " +
                    s"no metadata in the cache; this is not expected"
                  fatal(errorMsg)
                  throw new IllegalStateException(errorMsg)

                case Some(epochAndMetadata) =>
                  if (epochAndMetadata.coordinatorEpoch == coordinatorEpoch) {
                    val txnMetadata = epochAndMetadata.transactionMetadata
                    txnMetadata.inLock {
                      if (txnMetadata.producerId != producerId)
                        Left(Errors.INVALID_PRODUCER_ID_MAPPING)
                      else if (txnMetadata.producerEpoch != producerEpoch)
                        Left(Errors.INVALID_PRODUCER_EPOCH)
                      else if (txnMetadata.pendingTransitionInProgress)
                        Left(Errors.CONCURRENT_TRANSACTIONS)
                      else txnMetadata.state match {
                        case Empty| Ongoing | CompleteCommit | CompleteAbort =>
                          logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
                        case PrepareCommit =>  // 预完成提交
                          if (txnMarkerResult != TransactionResult.COMMIT)
                            logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
                          else
                            Right(txnMetadata, txnMetadata.prepareComplete(time.milliseconds()))
                        case PrepareAbort =>   // 预完成中断
                          if (txnMarkerResult != TransactionResult.ABORT)
                            logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
                          else
                            Right(txnMetadata, txnMetadata.prepareComplete(time.milliseconds()))
                        case Dead | PrepareEpochFence =>
                          val errorMsg = s"Found transactionalId $transactionalId with state ${txnMetadata.state}. " +
                            s"This is illegal as we should never have transitioned to this state."
                          fatal(errorMsg)
                          throw new IllegalStateException(errorMsg)

                      }
                    }
                  } else {
                    debug(s"The transaction coordinator epoch has changed to ${epochAndMetadata.coordinatorEpoch} after $txnMarkerResult was " +
                      s"successfully appended to the log for $transactionalId with old epoch $coordinatorEpoch")
                    Left(Errors.NOT_COORDINATOR)
                  }
              }

              preSendResult match { // 预完成和完成
                case Left(err) =>
                  info(s"Aborting sending of transaction markers after appended $txnMarkerResult to transaction log and returning $err error to client for $transactionalId's EndTransaction request")
                  responseCallback(err)

                case Right((txnMetadata, newPreSendMetadata)) =>
                  // 我们可以立即响应客户端，如果日志追加成功，则继续写入txn标记
                  responseCallback(Errors.NONE)
                  // 请求事务请求完成需要向其他broker发送事务marker请求
                  txnMarkerChannelManager.addTxnMarkersToSend(transactionalId, coordinatorEpoch, txnMarkerResult, txnMetadata, newPreSendMetadata)
              }
            } else {
              info(s"Aborting sending of transaction markers and returning $error error to client for $transactionalId's EndTransaction request of $txnMarkerResult, " +
                s"since appending $newMetadata to transaction log with coordinator epoch $coordinatorEpoch failed")

              responseCallback(error)
            }
          }

          txnManager.appendTransactionToLog(transactionalId, coordinatorEpoch, newMetadata, sendTxnMarkersCallback) // 完成事务
      }
    }
  }

  def transactionTopicConfigs: Properties = txnManager.transactionTopicConfigs

  def partitionFor(transactionalId: String): Int = txnManager.partitionFor(transactionalId)

  // 中断超时异常
  private def abortTimedOutTransactions(): Unit = {
    txnManager.timedOutTransactions().foreach { txnIdAndPidEpoch =>
      txnManager.getTransactionState(txnIdAndPidEpoch.transactionalId).right.flatMap {
        case None =>
          error(s"Could not find transaction metadata when trying to timeout transaction with transactionalId " +
            s"${txnIdAndPidEpoch.transactionalId}. ProducerId: ${txnIdAndPidEpoch.producerId}. ProducerEpoch: " +
            s"${txnIdAndPidEpoch.producerEpoch}")
          Left(Errors.INVALID_TXN_STATE)

        case Some(epochAndTxnMetadata) =>
          val txnMetadata = epochAndTxnMetadata.transactionMetadata
          val transitMetadata = txnMetadata.inLock {
            if (txnMetadata.producerId != txnIdAndPidEpoch.producerId) {
              error(s"Found incorrect producerId when expiring transactionalId: ${txnIdAndPidEpoch.transactionalId}. " +
                s"Expected producerId: ${txnIdAndPidEpoch.producerId}. Found producerId: " +
                s"${txnMetadata.producerId}")
              Left(Errors.INVALID_PRODUCER_ID_MAPPING)
            } else if (txnMetadata.pendingTransitionInProgress) {
              Left(Errors.CONCURRENT_TRANSACTIONS)
            } else {
              Right(txnMetadata.prepareFenceProducerEpoch()) // 改变状态  PrepareEpochFence
            }
          }
          transitMetadata match {  // 处理超时的异常
            case Right(txnTransitMetadata) =>
              handleEndTransaction(txnMetadata.transactionalId,
                txnTransitMetadata.producerId,
                txnTransitMetadata.producerEpoch,
                TransactionResult.ABORT,
                {
                  case Errors.NONE =>
                    info(s"Completed rollback ongoing transaction of transactionalId: ${txnIdAndPidEpoch.transactionalId} due to timeout")
                  case e @ (Errors.INVALID_PRODUCER_ID_MAPPING |
                            Errors.INVALID_PRODUCER_EPOCH |
                            Errors.CONCURRENT_TRANSACTIONS) =>
                    debug(s"Rolling back ongoing transaction of transactionalId: ${txnIdAndPidEpoch.transactionalId} has aborted due to ${e.exceptionName}")
                  case e =>
                    warn(s"Rolling back ongoing transaction of transactionalId: ${txnIdAndPidEpoch.transactionalId} failed due to ${e.exceptionName}")
                })
              Right(txnTransitMetadata)
            case (error) =>
              Left(error)
          }
      }
    }
  }

  /**
   * 启动逻辑在服务器启动的同时执行。
   */
  def startup(enableTransactionalIdExpiration: Boolean = true) {
    info("Starting up.")
    scheduler.startup()
    scheduler.schedule("transaction-abort",
      abortTimedOutTransactions, // 中断超时事务
      txnConfig.abortTimedOutTransactionsIntervalMs,
      txnConfig.abortTimedOutTransactionsIntervalMs
    )
    if (enableTransactionalIdExpiration)
      txnManager.enableTransactionalIdExpiration()
    txnMarkerChannelManager.start()
    isActive.set(true)

    info("Startup complete.")
  }

  /**
   * Shutdown logic executed at the same time when server shuts down.
   * Ordering of actions should be reversed from the startup process.
   */
  def shutdown() {
    info("Shutting down.")
    isActive.set(false)
    scheduler.shutdown()
    producerIdManager.shutdown()
    txnManager.shutdown()
    txnMarkerChannelManager.shutdown()
    info("Shutdown complete.")
  }
}

case class InitProducerIdResult(producerId: Long, producerEpoch: Short, error: Errors)
