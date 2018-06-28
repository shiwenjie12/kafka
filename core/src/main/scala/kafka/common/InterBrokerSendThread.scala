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
package kafka.common

import kafka.utils.ShutdownableThread
import org.apache.kafka.clients.{ClientResponse, NetworkClient, RequestCompletionHandler}
import org.apache.kafka.common.Node
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.utils.Time


/**
 *  Inter-broker发送线程使用非阻塞网络客户端的类。
 */
abstract class InterBrokerSendThread(name: String,
                                     networkClient: NetworkClient,
                                     time: Time,
                                     isInterruptible: Boolean = true)
  extends ShutdownableThread(name, isInterruptible) {

  // 构造的请求，用于发送
  def generateRequests(): Iterable[RequestAndCompletionHandler]

  override def shutdown(): Unit = {
    initiateShutdown()
    // wake up the thread in case it is blocked inside poll
    networkClient.wakeup()
    awaitShutdown()
  }

  override def doWork() {
    val now = time.milliseconds()
    var pollTimeout = Long.MaxValue

    try {
      for (request: RequestAndCompletionHandler <- generateRequests()) {
        val destination = Integer.toString(request.destination.id())
        val completionHandler = request.handler
        val clientRequest = networkClient.newClientRequest(destination,
          request.request,
          now,
          true,
          completionHandler)

        if (networkClient.ready(request.destination, now)) {  // 发送请求
          networkClient.send(clientRequest, now)
        } else { // 如果本地不能调用，则直接本地进行回调
          val header = clientRequest.makeHeader(request.request.latestAllowedVersion)
          // 断开连接的响应
          val disconnectResponse: ClientResponse = new ClientResponse(header, completionHandler, destination,
            now /* createdTimeMs */ , now /* receivedTimeMs */ , true /* disconnected */ , null /* versionMismatch */ ,
            null /* responseBody */)

          // poll timeout would be the minimum of connection delay if there are any dest yet to be reached;
          // otherwise it is infinity
          pollTimeout = Math.min(pollTimeout, networkClient.connectionDelay(request.destination, now))

          completionHandler.onComplete(disconnectResponse)
        }
      }
      networkClient.poll(pollTimeout, now)
    } catch {
      case e: FatalExitError => throw e
      case t: Throwable =>
        error(s"unhandled exception caught in InterBrokerSendThread", t)
        // rethrow any unhandled exceptions as FatalExitError so the JVM will be terminated
        // as we will be in an unknown state with potentially some requests dropped and not
        // being able to make progress. Known and expected Errors should have been appropriately
        // dealt with already.
        throw new FatalExitError()
    }
  }

  def wakeup(): Unit = networkClient.wakeup()

}

// 包含请求和完成处理器
case class RequestAndCompletionHandler(destination: Node, request: AbstractRequest.Builder[_ <: AbstractRequest],
                                       handler: RequestCompletionHandler)