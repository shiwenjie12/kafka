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

package kafka.controller

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.locks.ReentrantLock

import kafka.metrics.KafkaTimer
import kafka.utils.CoreUtils.inLock
import kafka.utils.ShutdownableThread

import scala.collection._

object ControllerEventManager {
  val ControllerEventThreadName = "controller-event-thread"
}
// 控制器事件管理
class ControllerEventManager(controllerId: Int, rateAndTimeMetrics: Map[ControllerState, KafkaTimer],
                             eventProcessedListener: ControllerEvent => Unit) {

  @volatile private var _state: ControllerState = ControllerState.Idle
  private val putLock = new ReentrantLock()
  // 事件的阻塞队列
  private val queue = new LinkedBlockingQueue[ControllerEvent]
  private val thread = new ControllerEventThread(ControllerEventManager.ControllerEventThreadName)

  def state: ControllerState = _state

  def start(): Unit = thread.start()

  def close(): Unit = {
    clearAndPut(KafkaController.ShutdownEventThread)
    thread.awaitShutdown()
  }

  def put(event: ControllerEvent): Unit = inLock(putLock) {
    queue.put(event)
  }

  def clearAndPut(event: ControllerEvent): Unit = inLock(putLock) {
    queue.clear()
    queue.put(event)
  }

  class ControllerEventThread(name: String) extends ShutdownableThread(name = name, isInterruptible = false) {
    logIdent = s"[ControllerEventThread controllerId=$controllerId] "

    override def doWork(): Unit = {
      queue.take() match {
        case KafkaController.ShutdownEventThread => initiateShutdown() // 初始化关闭线程
        case controllerEvent =>
          _state = controllerEvent.state // 控制器状态

          try {
            rateAndTimeMetrics(state).time { // 状态的速率度量器和timer
              controllerEvent.process() // 执行事件任务
            }
          } catch {
            case e: Throwable => error(s"Error processing event $controllerEvent", e)
          }

          try eventProcessedListener(controllerEvent) // 更新控制器度量
          catch {
            case e: Throwable => error(s"Error while invoking listener for processed event $controllerEvent", e)
          }

          _state = ControllerState.Idle
      }
    }
  }

}
