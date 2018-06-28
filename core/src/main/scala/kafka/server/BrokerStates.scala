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

/**
 * Broker states are the possible state that a kafka broker can be in.
 * A broker should be only in one state at a time.
 * The expected state transition with the following defined states is:
 *
 *                +-----------+
 *                |Not Running|
 *                +-----+-----+
 *                      |
 *                      v
 *                +-----+-----+
 *                |Starting   +--+
 *                +-----+-----+  | +----+------------+
 *                      |        +>+RecoveringFrom   |
 *                      v          |UncleanShutdown  |
 *               +-------+-------+ +-------+---------+
 *               |RunningAsBroker|            |
 *               +-------+-------+<-----------+
 *                       |
 *                       v
 *                +-----+------------+
 *                |PendingControlled |
 *                |Shutdown          |
 *                +-----+------------+
 *                      |
 *                      v
 *               +-----+----------+
 *               |BrokerShutting  |
 *               |Down            |
 *               +-----+----------+
 *                     |
 *                     v
 *               +-----+-----+
 *               |Not Running|
 *               +-----------+
 *
 * 对于不同场景的自定义卡夫卡状态，也允许使用自定义状态。
 */
sealed trait BrokerStates { def state: Byte }
case object NotRunning extends BrokerStates { val state: Byte = 0 }
case object Starting extends BrokerStates { val state: Byte = 1 }
case object RecoveringFromUncleanShutdown extends BrokerStates { val state: Byte = 2 }
case object RunningAsBroker extends BrokerStates { val state: Byte = 3 }
case object PendingControlledShutdown extends BrokerStates { val state: Byte = 6 }
case object BrokerShuttingDown extends BrokerStates { val state: Byte = 7 }

/**
  * 代理服务器的当前状态，默认（NotRunning）
  */
case class BrokerState() {
  @volatile var currentState: Byte = NotRunning.state

  def newState(newState: BrokerStates) {
    this.newState(newState.state)
  }

  // Allowing undefined custom state
  def newState(newState: Byte) {
    currentState = newState
  }
}
