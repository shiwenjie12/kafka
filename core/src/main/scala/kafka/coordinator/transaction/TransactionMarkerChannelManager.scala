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


import kafka.common.{InterBrokerSendThread, RequestAndCompletionHandler}
import kafka.metrics.KafkaMetricsGroup
import kafka.server.{DelayedOperationPurgatory, KafkaConfig, MetadataCache}
import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.clients._
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.requests.{TransactionResult, WriteTxnMarkersRequest}
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.WriteTxnMarkersRequest.TxnMarkerEntry
import com.yammer.metrics.core.Gauge
import java.util
import java.util.concurrent.{BlockingQueue, ConcurrentHashMap, LinkedBlockingQueue}

import collection.JavaConverters._
import scala.collection.{concurrent, immutable}

object TransactionMarkerChannelManager {
  def apply(config: KafkaConfig,
            metrics: Metrics,
            metadataCache: MetadataCache,
            txnStateManager: TransactionStateManager,
            txnMarkerPurgatory: DelayedOperationPurgatory[DelayedTxnMarker],
            time: Time,
            logContext: LogContext): TransactionMarkerChannelManager = {
    val channelBuilder = ChannelBuilders.clientChannelBuilder(
      config.interBrokerSecurityProtocol,
      JaasContext.Type.SERVER,
      config,
      config.interBrokerListenerName,
      config.saslMechanismInterBrokerProtocol,
      config.saslInterBrokerHandshakeRequestEnable
    )
    val selector = new Selector(
      NetworkReceive.UNLIMITED,
      config.connectionsMaxIdleMs,
      metrics,
      time,
      "txn-marker-channel",
      Map.empty[String, String].asJava,
      false,
      channelBuilder,
      logContext
    )
    val networkClient = new NetworkClient(
      selector,
      new ManualMetadataUpdater(),
      s"broker-${config.brokerId}-txn-marker-sender",
      1,
      50,
      50,
      Selectable.USE_DEFAULT_BUFFER_SIZE,
      config.socketReceiveBufferBytes,
      config.requestTimeoutMs,
      time,
      false,
      new ApiVersions,
      logContext
    )

    new TransactionMarkerChannelManager(config,
      metadataCache,
      networkClient,
      txnStateManager,
      txnMarkerPurgatory,
      time
    )
  }

}

// 每个broker的事务标记队列
class TxnMarkerQueue(@volatile var destination: Node) {

  // 跟踪每个txn主题分区的请求，以便我们可以在分区迁移过程中轻松清除队列
  private val markersPerTxnTopicPartition = new ConcurrentHashMap[Int, BlockingQueue[TxnIdAndMarkerEntry]]().asScala

  def removeMarkersForTxnTopicPartition(partition: Int): Option[BlockingQueue[TxnIdAndMarkerEntry]] = {
    markersPerTxnTopicPartition.remove(partition)
  }

  // 在指定的事务，添加事务标记
  def addMarkers(txnTopicPartition: Int, txnIdAndMarker: TxnIdAndMarkerEntry): Unit = {
    val queue = CoreUtils.atomicGetOrUpdate(markersPerTxnTopicPartition, txnTopicPartition,
        new LinkedBlockingQueue[TxnIdAndMarkerEntry]())
    queue.add(txnIdAndMarker)
  }

  def forEachTxnTopicPartition[B](f:(Int, BlockingQueue[TxnIdAndMarkerEntry]) => B): Unit =
    markersPerTxnTopicPartition.foreach { case (partition, queue) =>
      if (!queue.isEmpty) f(partition, queue)
    }

  def totalNumMarkers: Int = markersPerTxnTopicPartition.values.foldLeft(0) { _ + _.size }

  // visible for testing
  def totalNumMarkers(txnTopicPartition: Int): Int = markersPerTxnTopicPartition.get(txnTopicPartition).fold(0)(_.size)
}

// 事务通道管理器，用于通知其他broker
class TransactionMarkerChannelManager(config: KafkaConfig,
                                      metadataCache: MetadataCache,
                                      networkClient: NetworkClient,
                                      txnStateManager: TransactionStateManager,
                                      txnMarkerPurgatory: DelayedOperationPurgatory[DelayedTxnMarker],
                                      time: Time) extends InterBrokerSendThread("TxnMarkerSenderThread-" + config.brokerId, networkClient, time) with Logging with KafkaMetricsGroup {

  this.logIdent = "[Transaction Marker Channel Manager " + config.brokerId + "]: "

  private val interBrokerListenerName: ListenerName = config.interBrokerListenerName

  // 每个broker的事务标记队列
  private val markersQueuePerBroker: concurrent.Map[Int, TxnMarkerQueue] = new ConcurrentHashMap[Int, TxnMarkerQueue]().asScala

  private val markersQueueForUnknownBroker = new TxnMarkerQueue(Node.noNode)

  // 用于重新入队
  private val txnLogAppendRetryQueue = new LinkedBlockingQueue[TxnLogAppend]()

  newGauge(
    "UnknownDestinationQueueSize",
    new Gauge[Int] {
      def value: Int = markersQueueForUnknownBroker.totalNumMarkers
    }
  )

  newGauge(
    "LogAppendRetryQueueSize",
    new Gauge[Int] {
      def value: Int = txnLogAppendRetryQueue.size
    }
  )

  // 构造请求，用于发送
  override def generateRequests() = drainQueuedTransactionMarkers()

  override def shutdown(): Unit = {
    super.shutdown()
    txnMarkerPurgatory.shutdown()
    markersQueuePerBroker.clear()
  }

  // visible for testing
  private[transaction] def queueForBroker(brokerId: Int) = {
    markersQueuePerBroker.get(brokerId)
  }

  // visible for testing
  private[transaction] def queueForUnknownBroker = markersQueueForUnknownBroker

  // 添加事务标记到broker
  private[transaction] def addMarkersForBroker(broker: Node, txnTopicPartition: Int, txnIdAndMarker: TxnIdAndMarkerEntry) {
    val brokerId = broker.id

    // we do not synchronize on the update of the broker node with the enqueuing,
    // since even if there is a race condition we will just retry
    val brokerRequestQueue = CoreUtils.atomicGetOrUpdate(markersQueuePerBroker, brokerId,
        new TxnMarkerQueue(broker))
    brokerRequestQueue.destination = broker
    brokerRequestQueue.addMarkers(txnTopicPartition, txnIdAndMarker)

    trace(s"Added marker ${txnIdAndMarker.txnMarkerEntry} for transactional id ${txnIdAndMarker.txnId} to destination broker $brokerId")
  }

  def retryLogAppends(): Unit = {
    val txnLogAppendRetries: java.util.List[TxnLogAppend] = new util.ArrayList[TxnLogAppend]()
    txnLogAppendRetryQueue.drainTo(txnLogAppendRetries) // 一次性移除当前阻塞队列的数据，并复制到其他队列
    txnLogAppendRetries.asScala.foreach { txnLogAppend =>
      debug(s"Retry appending $txnLogAppend transaction log")
      tryAppendToLog(txnLogAppend)
    }
  }

  // 获取用于发送的请求处理队列
  private[transaction] def drainQueuedTransactionMarkers(): Iterable[RequestAndCompletionHandler] = {
    retryLogAppends()
    val txnIdAndMarkerEntries: java.util.List[TxnIdAndMarkerEntry] = new util.ArrayList[TxnIdAndMarkerEntry]()
    markersQueueForUnknownBroker.forEachTxnTopicPartition { case (_, queue) => // 将无节点事务添加到临时队列中
      queue.drainTo(txnIdAndMarkerEntries)
    }

    for (txnIdAndMarker: TxnIdAndMarkerEntry <- txnIdAndMarkerEntries.asScala) { // 无节点
      val transactionalId = txnIdAndMarker.txnId
      val producerId = txnIdAndMarker.txnMarkerEntry.producerId
      val producerEpoch = txnIdAndMarker.txnMarkerEntry.producerEpoch
      val txnResult = txnIdAndMarker.txnMarkerEntry.transactionResult
      val coordinatorEpoch = txnIdAndMarker.txnMarkerEntry.coordinatorEpoch
      val topicPartitions = txnIdAndMarker.txnMarkerEntry.partitions.asScala.toSet

      // 重新发送
      addTxnMarkersToBrokerQueue(transactionalId, producerId, producerEpoch, txnResult, coordinatorEpoch, topicPartitions)
    }

    markersQueuePerBroker.values.map { brokerRequestQueue =>
      val txnIdAndMarkerEntries = new util.ArrayList[TxnIdAndMarkerEntry]()
      brokerRequestQueue.forEachTxnTopicPartition { case (_, queue) =>
        queue.drainTo(txnIdAndMarkerEntries)
      }
      (brokerRequestQueue.destination, txnIdAndMarkerEntries) // 节点和事务标记实体
    }.filter { case (_, entries) => !entries.isEmpty }.map { case (node, entries) => // 按照节点进行划分
      val markersToSend = entries.asScala.map(_.txnMarkerEntry).asJava
      val requestCompletionHandler = new TransactionMarkerRequestCompletionHandler(node.id, txnStateManager, this, entries)
      RequestAndCompletionHandler(node, new WriteTxnMarkersRequest.Builder(markersToSend), requestCompletionHandler)
    }
  }

  // 添加事务到发送队列
  def addTxnMarkersToSend(transactionalId: String,
                          coordinatorEpoch: Int,
                          txnResult: TransactionResult,
                          txnMetadata: TransactionMetadata,
                          newMetadata: TxnTransitMetadata): Unit = {

    // 添加日志的回调，延迟操作的完成回调
    def appendToLogCallback(error: Errors): Unit = {
      error match {
        case Errors.NONE =>
          trace(s"Completed sending transaction markers for $transactionalId as $txnResult")

          txnStateManager.getTransactionState(transactionalId) match {
            case Left(Errors.NOT_COORDINATOR) =>
              info(s"No longer the coordinator for $transactionalId with coordinator epoch $coordinatorEpoch; cancel appending $newMetadata to transaction log")

            case Left(Errors.COORDINATOR_LOAD_IN_PROGRESS) =>
              info(s"Loading the transaction partition that contains $transactionalId while my current coordinator epoch is $coordinatorEpoch; " +
                s"so cancel appending $newMetadata to transaction log since the loading process will continue the remaining work")

            case Left(unexpectedError) =>
              throw new IllegalStateException(s"Unhandled error $unexpectedError when fetching current transaction state")

            case Right(Some(epochAndMetadata)) =>
              if (epochAndMetadata.coordinatorEpoch == coordinatorEpoch) {  // 添加到日志
                debug(s"Sending $transactionalId's transaction markers for $txnMetadata with coordinator epoch $coordinatorEpoch succeeded, trying to append complete transaction log now")

                tryAppendToLog(TxnLogAppend(transactionalId, coordinatorEpoch, txnMetadata, newMetadata))
              } else {
                info(s"The cached metadata $txnMetadata has changed to $epochAndMetadata after completed sending the markers with coordinator " +
                  s"epoch $coordinatorEpoch; abort transiting the metadata to $newMetadata as it may have been updated by another process")
              }

            case Right(None) =>
              val errorMsg = s"The coordinator still owns the transaction partition for $transactionalId, but there is " +
                s"no metadata in the cache; this is not expected"
              fatal(errorMsg)
              throw new IllegalStateException(errorMsg)
          }

        case other =>
          val errorMsg = s"Unexpected error ${other.exceptionName} before appending to txn log for $transactionalId"
          fatal(errorMsg)
          throw new IllegalStateException(errorMsg)
      }
    }

    val delayedTxnMarker = new DelayedTxnMarker(txnMetadata, appendToLogCallback, txnStateManager.stateReadLock)
    txnMarkerPurgatory.tryCompleteElseWatch(delayedTxnMarker, Seq(transactionalId))  // 使用事务id进行注册watch

    // 将事务标记发送到其他broker队列
    addTxnMarkersToBrokerQueue(transactionalId, txnMetadata.producerId, txnMetadata.producerEpoch, txnResult, coordinatorEpoch, txnMetadata.topicPartitions.toSet)
  }

  private def tryAppendToLog(txnLogAppend: TxnLogAppend) = {
    // try to append to the transaction log
    def appendCallback(error: Errors): Unit =
      error match {
        case Errors.NONE =>
          trace(s"Completed transaction for ${txnLogAppend.transactionalId} with coordinator epoch ${txnLogAppend.coordinatorEpoch}, final state after commit: ${txnLogAppend.txnMetadata.state}")

        case Errors.NOT_COORDINATOR =>
          info(s"No longer the coordinator for transactionalId: ${txnLogAppend.transactionalId} while trying to append to transaction log, skip writing to transaction log")

        case Errors.COORDINATOR_NOT_AVAILABLE =>
          info(s"Not available to append $txnLogAppend: possible causes include ${Errors.UNKNOWN_TOPIC_OR_PARTITION}, ${Errors.NOT_ENOUGH_REPLICAS}, " +
            s"${Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND} and ${Errors.REQUEST_TIMED_OUT}; retry appending")

          // 入队，用于重试
          txnLogAppendRetryQueue.add(txnLogAppend)

        case Errors.COORDINATOR_LOAD_IN_PROGRESS =>
          info(s"Coordinator is loading the partition ${txnStateManager.partitionFor(txnLogAppend.transactionalId)} and hence cannot complete append of $txnLogAppend; " +
            s"skip writing to transaction log as the loading process should complete it")

        case other: Errors =>
          val errorMsg = s"Unexpected error ${other.exceptionName} while appending to transaction log for ${txnLogAppend.transactionalId}"
          fatal(errorMsg)
          throw new IllegalStateException(errorMsg)
      }

    // 添加事务到日志
    txnStateManager.appendTransactionToLog(txnLogAppend.transactionalId, txnLogAppend.coordinatorEpoch, txnLogAppend.newMetadata, appendCallback,
      _ == Errors.COORDINATOR_NOT_AVAILABLE)
  }

  def addTxnMarkersToBrokerQueue(transactionalId: String, producerId: Long, producerEpoch: Short,
                                 result: TransactionResult, coordinatorEpoch: Int,
                                 topicPartitions: immutable.Set[TopicPartition]): Unit = {
    val txnTopicPartition = txnStateManager.partitionFor(transactionalId)  // 事务的分区id
    val partitionsByDestination: immutable.Map[Option[Node], immutable.Set[TopicPartition]] = topicPartitions.groupBy { topicPartition: TopicPartition =>
      metadataCache.getPartitionLeaderEndpoint(topicPartition.topic, topicPartition.partition, interBrokerListenerName) // tp的领导者节点
    }

    // 向本次事务中涉及到的tp，发送TxnMarkerEntry
    for ((broker: Option[Node], topicPartitions: immutable.Set[TopicPartition]) <- partitionsByDestination) {
      broker match {
        case Some(brokerNode) =>
          val marker = new TxnMarkerEntry(producerId, producerEpoch, coordinatorEpoch, result, topicPartitions.toList.asJava)
          val txnIdAndMarker = TxnIdAndMarkerEntry(transactionalId, marker)

          if (brokerNode == Node.noNode) {  // 添加到事务队列中
            // if the leader of the partition is known but node not available, put it into an unknown broker queue
            // and let the sender thread to look for its broker and migrate them later
            markersQueueForUnknownBroker.addMarkers(txnTopicPartition, txnIdAndMarker)
          } else {
            addMarkersForBroker(brokerNode, txnTopicPartition, txnIdAndMarker)
          }

        case None => // 无领导者的tp
          txnStateManager.getTransactionState(transactionalId) match {
            case Left(error) =>
              info(s"Encountered $error trying to fetch transaction metadata for $transactionalId with coordinator epoch $coordinatorEpoch; cancel sending markers to its partition leaders")
              txnMarkerPurgatory.cancelForKey(transactionalId)  // 取消事务

            case Right(Some(epochAndMetadata)) =>
              if (epochAndMetadata.coordinatorEpoch != coordinatorEpoch) { // epoch不符合预期，取消
                info(s"The cached metadata has changed to $epochAndMetadata (old coordinator epoch is $coordinatorEpoch) since preparing to send markers; cancel sending markers to its partition leaders")
                txnMarkerPurgatory.cancelForKey(transactionalId)
              } else {
                // 如果分区的首领未知，则跳过发送txn标记，因为分区可能已被删除
                info(s"Couldn't find leader endpoint for partitions $topicPartitions while trying to send transaction markers for " +
                  s"$transactionalId, these partitions are likely deleted already and hence can be skipped")

                val txnMetadata = epochAndMetadata.transactionMetadata

                txnMetadata.inLock { // 移除事务中tp
                  topicPartitions.foreach(txnMetadata.removePartition)
                }

                txnMarkerPurgatory.checkAndComplete(transactionalId) // 完成事务
              }

            case Right(None) =>
              val errorMsg = s"The coordinator still owns the transaction partition for $transactionalId, but there is " +
                s"no metadata in the cache; this is not expected"
              fatal(errorMsg)
              throw new IllegalStateException(errorMsg)

          }
      }
    }

    wakeup()
  }

  def removeMarkersForTxnTopicPartition(txnTopicPartitionId: Int): Unit = {
    markersQueueForUnknownBroker.removeMarkersForTxnTopicPartition(txnTopicPartitionId).foreach { queue =>
      for (entry: TxnIdAndMarkerEntry <- queue.asScala)
        removeMarkersForTxnId(entry.txnId)
    }

    markersQueuePerBroker.foreach { case(_, brokerQueue) =>
      brokerQueue.removeMarkersForTxnTopicPartition(txnTopicPartitionId).foreach { queue =>
        for (entry: TxnIdAndMarkerEntry <- queue.asScala)
          removeMarkersForTxnId(entry.txnId)
      }
    }
  }

  def removeMarkersForTxnId(transactionalId: String): Unit = {
    // 我们不需要清除队列，因为它应该已经被发送者线程耗尽了
    txnMarkerPurgatory.cancelForKey(transactionalId)
  }

  def completeSendMarkersForTxnId(transactionalId: String): Unit = {
    txnMarkerPurgatory.checkAndComplete(transactionalId)
  }
}

// 事务id和标记实体
case class TxnIdAndMarkerEntry(txnId: String, txnMarkerEntry: TxnMarkerEntry)

case class TxnLogAppend(transactionalId: String, coordinatorEpoch: Int, txnMetadata: TransactionMetadata, newMetadata: TxnTransitMetadata) {

  override def toString: String = {
    "TxnLogAppend(" +
      s"transactionalId=$transactionalId, " +
      s"coordinatorEpoch=$coordinatorEpoch, " +
      s"txnMetadata=$txnMetadata, " +
      s"newMetadata=$newMetadata)"
  }
}
