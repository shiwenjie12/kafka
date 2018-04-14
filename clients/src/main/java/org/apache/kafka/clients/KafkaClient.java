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
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.requests.AbstractRequest;

import java.io.Closeable;
import java.util.List;

/**
 * The interface for {@link NetworkClient}
 */
public interface KafkaClient extends Closeable {

    /**
     * 检查当前是否准备向指定节点发送另一个请求，但如果没有，请不要尝试连接。
     * @param node The node to check
     * @param now The current timestamp
     */
    boolean isReady(Node node, long now);

    /**
     * 初始化一个给定节点的连接，如果早已连接则返回true，只有当轮询被调用时，节点的就绪状态才会改变。
     *
     * @param node The node to connect to.
     * @param now The current time
     * @return true iff we are ready to immediately initiate the sending of another request to the given node.
     */
    boolean ready(Node node, long now);

    /**
     * 返回在试图发送数据之前基于连接状态等待的毫秒数。
     * 当断开连接时，这意味着重新连接退避时间。连接或连接时，它处理慢/中断连接。
     *
     * @param node The node to check
     * @param now The current timestamp
     * @return The number of milliseconds to wait.
     */
    long connectionDelay(Node node, long now);

    /**
     * Check if the connection of the node has failed, based on the connection state. Such connection failure are
     * usually transient and can be resumed in the next {@link #ready(org.apache.kafka.common.Node, long)} }
     * call, but there are cases where transient failures needs to be caught and re-acted upon.
     * 基于连接状态检查节点的连接是否失败。
     * 这样的连接失败通常是暂时的，可以在之后{{@link #ready(org.apache.kafka.common.Node, long)} }调用，
     * 但有时候失败需要被重新采取行动。
     *
     * @param node the node to check
     * @return true iff the connection has failed and the node is disconnected
     */
    boolean connectionFailed(Node node);

    /**
     * Check if authentication to this node has failed, based on the connection state. Authentication failures are
     * propagated without any retries.
     *
     * @param node the node to check
     * @return an AuthenticationException iff authentication has failed, null otherwise
     */
    AuthenticationException authenticationException(Node node);

    /**
     * 按给定的发送请求排队。只能在就绪连接上发送请求。
     * @param request The request
     * @param now The current timestamp
     */
    void send(ClientRequest request, long now);

    /**
     * 从套接字执行实际的读写操作。
     *
     * @param timeout The maximum amount of time to wait for responses in ms, must be non-negative. The implementation
     *                is free to use a lower value if appropriate (common reasons for this are a lower request or
     *                metadata update timeout)
     * @param now The current time in ms
     * @throws IllegalStateException If a request is sent to an unready node
     */
    List<ClientResponse> poll(long timeout, long now);

    /**
     * Disconnects the connection to a particular node, if there is one.
     * Any pending ClientRequests for this connection will receive disconnections.
     *
     * @param nodeId The id of the node
     */
    void disconnect(String nodeId);

    /**
     * Closes the connection to a particular node (if there is one).
     * All requests on the connection will be cleared.  ClientRequest callbacks will not be invoked
     * for the cleared requests, nor will they be returned from poll().
     *
     * @param nodeId The id of the node
     */
    void close(String nodeId);

    /**
     * Choose the node with the fewest outstanding requests. This method will prefer a node with an existing connection,
     * but will potentially choose a node for which we don't yet have a connection if all existing connections are in
     * use.
     *
     * @param now The current time in ms
     * @return The node with the fewest in-flight requests.
     */
    Node leastLoadedNode(long now);

    /**
     * The number of currently in-flight requests for which we have not yet returned a response
     */
    int inFlightRequestCount();

    /**
     * Return true if there is at least one in-flight request and false otherwise.
     */
    boolean hasInFlightRequests();

    /**
     * Get the total in-flight requests for a particular node
     *
     * @param nodeId The id of the node
     */
    int inFlightRequestCount(String nodeId);

    /**
     * Return true if there is at least one in-flight request for a particular node and false otherwise.
     */
    boolean hasInFlightRequests(String nodeId);

    /**
     * Return true if there is at least one node with connection in ready state and false otherwise.
     */
    boolean hasReadyNodes();

    /**
     * Wake up the client if it is currently blocked waiting for I/O
     */
    void wakeup();

    /**
     * Create a new ClientRequest.
     *
     * @param nodeId the node to send to
     * @param requestBuilder the request builder to use
     * @param createdTimeMs the time in milliseconds to use as the creation time of the request
     * @param expectResponse true iff we expect a response
     */
    ClientRequest newClientRequest(String nodeId, AbstractRequest.Builder<?> requestBuilder,
                                   long createdTimeMs, boolean expectResponse);

    /**
     * 创建一个新的ClientRequest
     *
     * @param nodeId the node to send to
     * @param requestBuilder the request builder to use
     * @param createdTimeMs the time in milliseconds to use as the creation time of the request
     * @param expectResponse true iff we expect a response
     * @param callback the callback to invoke when we get a response 当我们调用
     */
    ClientRequest newClientRequest(String nodeId, AbstractRequest.Builder<?> requestBuilder, long createdTimeMs,
                                   boolean expectResponse, RequestCompletionHandler callback);

}
