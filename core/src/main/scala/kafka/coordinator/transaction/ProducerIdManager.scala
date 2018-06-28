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

import java.nio.charset.StandardCharsets

import kafka.common.KafkaException
import kafka.utils.{Json, Logging}
import kafka.zk.{KafkaZkClient, ProducerIdBlockZNode}

import scala.collection.JavaConverters._

/**
  * ProducerIdManager是事务协调器的一部分，它以一种独特的方式提供ProducerIds，使得相同的producerId不会在多个事务协调器中分配两次。
  *
  * ProducerIds通过ZooKeeper进行管理，其中最新的producerId块由声明该块的管理器写入相应的ZK路径，
  * 其中写入的block_start和block_end都包含在内。
  *
  */
object ProducerIdManager extends Logging {
  val CurrentVersion: Long = 1L
  val PidBlockSize: Long = 1000L

  def generateProducerIdBlockJson(producerIdBlock: ProducerIdBlock): Array[Byte] = {
    Json.encodeAsBytes(Map("version" -> CurrentVersion,
      "broker" -> producerIdBlock.brokerId,
      "block_start" -> producerIdBlock.blockStartId.toString,
      "block_end" -> producerIdBlock.blockEndId.toString).asJava
    )
  }

  def parseProducerIdBlockData(jsonData: Array[Byte]): ProducerIdBlock = {
    try {
      Json.parseBytes(jsonData).map(_.asJsonObject).flatMap { js =>
        val brokerId = js("broker").to[Int]
        val blockStart = js("block_start").to[String].toLong
        val blockEnd = js("block_end").to[String].toLong
        Some(ProducerIdBlock(brokerId, blockStart, blockEnd))
      }.getOrElse(throw new KafkaException(s"Failed to parse the producerId block json $jsonData"))
    } catch {
      case e: java.lang.NumberFormatException =>
        // this should never happen: the written data has exceeded long type limit
        fatal(s"Read jason data $jsonData contains producerIds that have exceeded long type limit")
        throw e
    }
  }
}

// 生成者的块
case class ProducerIdBlock(brokerId: Int, blockStartId: Long, blockEndId: Long) {
  override def toString: String = {
    val producerIdBlockInfo = new StringBuilder
    producerIdBlockInfo.append("(brokerId:" + brokerId)
    producerIdBlockInfo.append(",blockStartProducerId:" + blockStartId)
    producerIdBlockInfo.append(",blockEndProducerId:" + blockEndId + ")")
    producerIdBlockInfo.toString()
  }
}

// 生产者的管理器
class ProducerIdManager(val brokerId: Int, val zkClient: KafkaZkClient) extends Logging {

  this.logIdent = "[ProducerId Manager " + brokerId + "]: "

  private var currentProducerIdBlock: ProducerIdBlock = null
  private var nextProducerId: Long = -1L

  // grab the first block of producerIds
  this synchronized {  //  初始化同步代码块
    getNewProducerIdBlock()
    nextProducerId = currentProducerIdBlock.blockStartId
  }

  // 产生新的服务器
  private def getNewProducerIdBlock(): Unit = {
    var zkWriteComplete = false
    while (!zkWriteComplete) {
      // 再次从zookeeper刷新当前producerId块
      val (dataOpt, zkVersion) = zkClient.getDataAndVersion(ProducerIdBlockZNode.path)

      // generate the new producerId block
      currentProducerIdBlock = dataOpt match {
        case Some(data) =>
          val currProducerIdBlock = ProducerIdManager.parseProducerIdBlockData(data)
          debug(s"Read current producerId block $currProducerIdBlock, Zk path version $zkVersion")

          if (currProducerIdBlock.blockEndId > Long.MaxValue - ProducerIdManager.PidBlockSize) {
            // we have exhausted all producerIds (wow!), treat it as a fatal error
            fatal(s"Exhausted all producerIds as the next block's end producerId is will has exceeded long type limit (current block end producerId is ${currProducerIdBlock.blockEndId})")
            throw new KafkaException("Have exhausted all producerIds.")
          }

          ProducerIdBlock(brokerId, currProducerIdBlock.blockEndId + 1L, currProducerIdBlock.blockEndId + ProducerIdManager.PidBlockSize)
        case None =>
          debug(s"There is no producerId block yet (Zk path version $zkVersion), creating the first block")
          ProducerIdBlock(brokerId, 0L, ProducerIdManager.PidBlockSize - 1)
      }

      val newProducerIdBlockData = ProducerIdManager.generateProducerIdBlockJson(currentProducerIdBlock)

      // 尝试将新的producerId块写入zookeeper
      val (succeeded, version) = zkClient.conditionalUpdatePath(ProducerIdBlockZNode.path,
        newProducerIdBlockData, zkVersion, Some(checkProducerIdBlockZkData))
      zkWriteComplete = succeeded

      if (zkWriteComplete)
        info(s"Acquired new producerId block $currentProducerIdBlock by writing to Zk with path version $version")
    }
  }

  private def checkProducerIdBlockZkData(zkClient: KafkaZkClient, path: String, expectedData: Array[Byte]): (Boolean, Int) = {
    try {
      val expectedPidBlock = ProducerIdManager.parseProducerIdBlockData(expectedData)
      zkClient.getDataAndVersion(ProducerIdBlockZNode.path) match {
        case (Some(data), zkVersion) =>
          val currProducerIdBLock = ProducerIdManager.parseProducerIdBlockData(data)
          (currProducerIdBLock == expectedPidBlock, zkVersion)
        case (None, _) => (false, -1)
      }
    } catch {
      case e: Exception =>
        warn(s"Error while checking for producerId block Zk data on path $path: expected data " +
          s"${new String(expectedData, StandardCharsets.UTF_8)}", e)
        (false, -1)
    }
  }

  def generateProducerId(): Long = {
    this synchronized {
      // 如果此块已耗尽，则抓取新的producerIds块
      if (nextProducerId > currentProducerIdBlock.blockEndId) {
        getNewProducerIdBlock()
        nextProducerId = currentProducerIdBlock.blockStartId + 1
      } else {
        nextProducerId += 1
      }

      nextProducerId - 1
    }
  }

  def shutdown() {
    info(s"Shutdown complete: last producerId assigned $nextProducerId")
  }
}
