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

package kafka.coordinator.group

import java.util

import kafka.utils.nonthreadsafe
import org.apache.kafka.common.protocol.Errors


case class MemberSummary(memberId: String,
                         clientId: String,
                         clientHost: String,
                         metadata: Array[Byte],
                         assignment: Array[Byte])

/**
 * 成员元数据包含以下元数据：
 *
 * 心跳元数据：
 * 1.协商心跳会话超时
 * 2.最新心跳的时间戳
 *
 * 协议元数据：
 * 1.支持的协议列表（按优先顺序排列）
 * 2.与每个协议相关的元数据
 *
 * 另外，它还包含以下状态信息：
 *
 * 1.等待重新平衡回调：当组处于prepare-rebalance状态时，如果成员发送了加入组请求，则其重新平衡回调将保留在元数据中
 * 2.等待同步回调：当组处于等待同步状态时，其同步回调将保存在元数据中，直到领导者提供组分配并且组转换为稳定
 *
 */
@nonthreadsafe
private[group] class MemberMetadata(val memberId: String,
                                    val groupId: String,
                                    val clientId: String,
                                    val clientHost: String,
                                    val rebalanceTimeoutMs: Int,
                                    val sessionTimeoutMs: Int,
                                    val protocolType: String,
                                    var supportedProtocols: List[(String, Array[Byte])]) {

  var assignment: Array[Byte] = Array.empty[Byte]
  var awaitingJoinCallback: JoinGroupResult => Unit = null
  var awaitingSyncCallback: (Array[Byte], Errors) => Unit = null
  var latestHeartbeat: Long = -1
  var isLeaving: Boolean = false

  def protocols = supportedProtocols.map(_._1).toSet

  /**
   * 获取与提供的协议相对应的元数据。
   */
  def metadata(protocol: String): Array[Byte] = {
    supportedProtocols.find(_._1 == protocol) match {
      case Some((_, metadata)) => metadata
      case None =>
        throw new IllegalArgumentException("Member does not support protocol")
    }
  }

  /**
   * 检查提供的协议元数据是否与当前存储的元数据匹配。
   */
  def matches(protocols: List[(String, Array[Byte])]): Boolean = {
    if (protocols.size != this.supportedProtocols.size)
      return false

    for (i <- protocols.indices) {
      val p1 = protocols(i)
      val p2 = supportedProtocols(i)
      if (p1._1 != p2._1 || !util.Arrays.equals(p1._2, p2._2))
        return false
    }
    true
  }

  def summary(protocol: String): MemberSummary = {
    MemberSummary(memberId, clientId, clientHost, metadata(protocol), assignment)
  }

  def summaryNoMetadata(): MemberSummary = {
    MemberSummary(memberId, clientId, clientHost, Array.empty[Byte], Array.empty[Byte])
  }

  /**
   * 为其中一个潜在的组协议投票。 这考虑到协议首选项，如支持的协议顺序所示，并返回集合中也包含的第一个协议首选项
    * candidates 候选人
   */
  def vote(candidates: Set[String]): String = {
    supportedProtocols.find({ case (protocol, _) => candidates.contains(protocol)}) match {
      case Some((protocol, _)) => protocol
      case None =>
        throw new IllegalArgumentException("Member does not support any of the candidate protocols")
    }
  }

  override def toString: String = {
    "MemberMetadata(" +
      s"memberId=$memberId, " +
      s"clientId=$clientId, " +
      s"clientHost=$clientHost, " +
      s"sessionTimeoutMs=$sessionTimeoutMs, " +
      s"rebalanceTimeoutMs=$rebalanceTimeoutMs, " +
      s"supportedProtocols=${supportedProtocols.map(_._1)}, " +
      ")"
  }

}
