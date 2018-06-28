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

package org.apache.kafka.clients;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.utils.Time;

import java.io.IOException;
import java.util.List;

/**
 * Provides additional utilities for {@link NetworkClient} (e.g. to implement blocking behaviour).
 */
public final class NetworkClientUtils {

    private NetworkClientUtils() {}

    /**
     * Checks whether the node is currently connected, first calling `client.poll` to ensure that any pending
     * disconnects have been processed.
     * 检查节点是否已连接，首先调用`client.poll`确保任何挂起的中断被处理。
     *
     * This method can be used to check the status of a connection prior to calling the blocking version to be able
     * to tell whether the latter completed a new connection.
     */
    public static boolean isReady(KafkaClient client, Node node, long currentTime) {
        client.poll(0, currentTime); // 马上进行处理
        return client.isReady(node, currentTime);
    }

    /**
     * 调用`client.poll`来放弃挂起的断开连接，后面跟着`client.ready`和0个或多个`client.poll调用，
     * 直到连接到`node`准备就绪，timeoutMs过期或连接失败。
     *
     * It returns `true` if the call completes normally or `false` if the timeoutMs expires. If the connection fails,
     * an `IOException` is thrown instead. Note that if the `NetworkClient` has been configured with a positive
     * connection timeoutMs, it is possible for this method to raise an `IOException` for a previous connection which
     * has recently disconnected. If authentication to the node fails, an `AuthenticationException` is thrown.
     *
     * 这个方法对于在非阻塞的`NetworkClient`之上实现阻塞行为很有用，请小心使用它。
     */
    public static boolean awaitReady(KafkaClient client, Node node, Time time, long timeoutMs) throws IOException {
        if (timeoutMs < 0) {
            throw new IllegalArgumentException("Timeout needs to be greater than 0");
        }
        long startTime = time.milliseconds();
        long expiryTime = startTime + timeoutMs;

        if (isReady(client, node, startTime) // 判断是否已经准备好
                ||  client.ready(node, startTime)) // 如果没有准备好，则进行准备
            return true;

        long attemptStartTime = time.milliseconds(); // 开始尝试时间
        while (!client.isReady(node, attemptStartTime) && attemptStartTime < expiryTime) {
            if (client.connectionFailed(node)) { // 连接失败
                throw new IOException("Connection to " + node + " failed.");
            }
            long pollTimeout = expiryTime - attemptStartTime;
            client.poll(pollTimeout, attemptStartTime);
            if (client.authenticationException(node) != null) // 授权失败
                throw client.authenticationException(node);
            attemptStartTime = time.milliseconds();
        }
        return client.isReady(node, attemptStartTime);
    }

    /**
     * 调用`client.send`，然后执行1个或多个`client.poll`调用，直到收到响应或发生断开（这可能发生在许多原因中，包括请求超时）。
     *
     * In case of a disconnection, an `IOException` is thrown.
     *
     * This method is useful for implementing blocking behaviour on top of the non-blocking `NetworkClient`, use it with
     * care.
     */
    public static ClientResponse sendAndReceive(KafkaClient client, ClientRequest request, Time time) throws IOException {
        client.send(request, time.milliseconds());
        while (true) {// 根据correlationId获取响应
            List<ClientResponse> responses = client.poll(Long.MAX_VALUE, time.milliseconds());
            for (ClientResponse response : responses) {
                if (response.requestHeader().correlationId() == request.correlationId()) {
                    if (response.wasDisconnected()) {
                        throw new IOException("Connection to " + response.destination() + " was disconnected before the response was read");
                    }
                    if (response.versionMismatch() != null) {
                        throw response.versionMismatch();
                    }
                    return response;
                }
            }
        }
    }
}
