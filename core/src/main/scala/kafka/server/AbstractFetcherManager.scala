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

package kafka.server

import kafka.utils.Logging
import kafka.cluster.BrokerEndPoint
import kafka.metrics.KafkaMetricsGroup
import com.yammer.metrics.core.Gauge
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils

import scala.collection.mutable
import scala.collection.{Map, Set}
import scala.collection.JavaConverters._

abstract class AbstractFetcherManager(protected val name: String, clientId: String, numFetchers: Int = 1)
  extends Logging with KafkaMetricsGroup {
  // map of (source broker_id, fetcher_id per source broker) => fetcher.
  // package private for test
  private[server] val fetcherThreadMap = new mutable.HashMap[BrokerIdAndFetcherId, AbstractFetcherThread]
  private val lock = new Object
  private var numFetchersPerBroker = numFetchers // 当前broker的feach线程数
  this.logIdent = "[" + name + "] "

  newGauge(
    "MaxLag",
    new Gauge[Long] {
      // current max lag across all fetchers/topics/partitions
      def value = fetcherThreadMap.foldLeft(0L)((curMaxAll, fetcherThreadMapEntry) => {
        fetcherThreadMapEntry._2.fetcherLagStats.stats.foldLeft(0L)((curMaxThread, fetcherLagStatsEntry) => {
          curMaxThread.max(fetcherLagStatsEntry._2.lag)
        }).max(curMaxAll)
      })
    },
    Map("clientId" -> clientId)
  )

  newGauge(
  "MinFetchRate", {
    new Gauge[Double] {
      // current min fetch rate across all fetchers/topics/partitions
      def value = {
        val headRate: Double =
          fetcherThreadMap.headOption.map(_._2.fetcherStats.requestRate.oneMinuteRate).getOrElse(0)

        fetcherThreadMap.foldLeft(headRate)((curMinAll, fetcherThreadMapEntry) => {
          fetcherThreadMapEntry._2.fetcherStats.requestRate.oneMinuteRate.min(curMinAll)
        })
      }
    }
  },
  Map("clientId" -> clientId)
  )

  // 重置线程池大小
  def resizeThreadPool(newSize: Int): Unit = {
    def migratePartitions(newSize: Int): Unit = { // 迁移分区
      fetcherThreadMap.foreach { case (id, thread) =>
        val removedPartitions = thread.partitionsAndOffsets // tp/broker/初始化偏移量
        removeFetcherForPartitions(removedPartitions.keySet) // 移除所有的分区fetcher
        if (id.fetcherId >= newSize)
          thread.shutdown()
        addFetcherForPartitions(removedPartitions)
      }
    }
    lock synchronized {
      val currentSize = numFetchersPerBroker
      info(s"Resizing fetcher thread pool size from $currentSize to $newSize")
      numFetchersPerBroker = newSize // 重新设置每个broker允许运行的线程数
      if (newSize != currentSize) {
        // We could just migrate some partitions explicitly to new threads. But this is currently
        // reassigning all partitions using the new thread size so that hash-based allocation
        // works with partition add/delete as it did before.
        migratePartitions(newSize)
      }
      shutdownIdleFetcherThreads()
    }
  }

  // Visibility for testing
  private[server] def getFetcherId(topic: String, partitionId: Int) : Int = {
    lock synchronized {
      Utils.abs(31 * topic.hashCode() + partitionId) % numFetchersPerBroker
    }
  }

  // 此方法仅由ReplicaAlterDirManager需要
  def markPartitionsForTruncation(brokerId: Int, topicPartition: TopicPartition, truncationOffset: Long) {
    lock synchronized {
      val fetcherId = getFetcherId(topicPartition.topic, topicPartition.partition) // 计算feacherId
      val brokerIdAndFetcherId = BrokerIdAndFetcherId(brokerId, fetcherId)
      fetcherThreadMap.get(brokerIdAndFetcherId).foreach { thread =>
        thread.markPartitionsForTruncation(topicPartition, truncationOffset)
      }
    }
  }

  // to be defined in subclass to create a specific fetcher
  def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): AbstractFetcherThread

  // 为分区添加feacher
  def addFetcherForPartitions(partitionAndOffsets: Map[TopicPartition, BrokerAndInitialOffset]) {
    lock synchronized {
      // 按照broker和fetcher,进行分组
      val partitionsPerFetcher = partitionAndOffsets.groupBy { case(topicPartition, brokerAndInitialFetchOffset) =>
        BrokerAndFetcherId(brokerAndInitialFetchOffset.broker, getFetcherId(topicPartition.topic, topicPartition.partition))}

      // 添加并启动获取线程
      def addAndStartFetcherThread(brokerAndFetcherId: BrokerAndFetcherId, brokerIdAndFetcherId: BrokerIdAndFetcherId) {
        val fetcherThread = createFetcherThread(brokerAndFetcherId.fetcherId, brokerAndFetcherId.broker)
        fetcherThreadMap.put(brokerIdAndFetcherId, fetcherThread)
        fetcherThread.start
      }

      for ((brokerAndFetcherId, initialFetchOffsets) <- partitionsPerFetcher) {
        val brokerIdAndFetcherId = BrokerIdAndFetcherId(brokerAndFetcherId.broker.id, brokerAndFetcherId.fetcherId)
        fetcherThreadMap.get(brokerIdAndFetcherId) match {
          case Some(f) if f.sourceBroker.host == brokerAndFetcherId.broker.host && f.sourceBroker.port == brokerAndFetcherId.broker.port =>
            // 重用fetcher线程
          case Some(f) =>  // 重启
            f.shutdown()
            addAndStartFetcherThread(brokerAndFetcherId, brokerIdAndFetcherId)
          case None => // 新增
            addAndStartFetcherThread(brokerAndFetcherId, brokerIdAndFetcherId)
        }

        fetcherThreadMap(brokerIdAndFetcherId).addPartitions(initialFetchOffsets.map { case (tp, brokerAndInitOffset) =>
          tp -> brokerAndInitOffset.initOffset
        })
      }
    }

    info("Added fetcher for partitions %s".format(partitionAndOffsets.map { case (topicPartition, brokerAndInitialOffset) =>
      "[" + topicPartition + ", initOffset " + brokerAndInitialOffset.initOffset + " to broker " + brokerAndInitialOffset.broker + "] "}))
  }

  // 移除关于分区的fetcher的信息
  def removeFetcherForPartitions(partitions: Set[TopicPartition]) {
    lock synchronized {
      for (fetcher <- fetcherThreadMap.values)
        fetcher.removePartitions(partitions)
    }
    info("Removed fetcher for partitions %s".format(partitions.mkString(",")))
  }

  // 关闭空闲的线程
  def shutdownIdleFetcherThreads() {
    lock synchronized {
      val keysToBeRemoved = new mutable.HashSet[BrokerIdAndFetcherId]
      for ((key, fetcher) <- fetcherThreadMap) {
        if (fetcher.partitionCount <= 0) { // fetcher线程的分区数
          fetcher.shutdown()
          keysToBeRemoved += key
        }
      }
      fetcherThreadMap --= keysToBeRemoved
    }
  }

  // 关闭所有的feacher线程
  def closeAllFetchers() {
    lock synchronized {
      for ( (_, fetcher) <- fetcherThreadMap) {
        fetcher.initiateShutdown()
      }

      for ( (_, fetcher) <- fetcherThreadMap) {
        fetcher.shutdown()
      }
      fetcherThreadMap.clear()
    }
  }
}

case class BrokerAndFetcherId(broker: BrokerEndPoint, fetcherId: Int)

case class BrokerAndInitialOffset(broker: BrokerEndPoint, initOffset: Long)

case class BrokerIdAndFetcherId(brokerId: Int, fetcherId: Int)
