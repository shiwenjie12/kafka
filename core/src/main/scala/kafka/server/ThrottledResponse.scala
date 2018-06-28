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

import java.util.concurrent.{TimeUnit, Delayed}

import org.apache.kafka.common.utils.Time


/**
 * 表示响应被延迟的请求。
 * @param time @Time instance to use
 * @param throttleTimeMs 与此请求相关联的延迟
 * @param callback Callback to trigger after delayTimeMs milliseconds
 */
private[server] class ThrottledResponse(val time: Time, val throttleTimeMs: Int, callback: Int => Unit) extends Delayed {
  val endTime = time.milliseconds + throttleTimeMs

  // 执行回调方法
  def execute() = callback(throttleTimeMs)

  // 返回还需要延迟的时间
  override def getDelay(unit: TimeUnit): Long = {
    unit.convert(endTime - time.milliseconds, TimeUnit.MILLISECONDS)
  }

  override def compareTo(d: Delayed): Int = {
    val other = d.asInstanceOf[ThrottledResponse]
    if (this.endTime < other.endTime) -1
    else if (this.endTime > other.endTime) 1
    else 0
  }
}
