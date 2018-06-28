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

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.ChannelState;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.CommonFields;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * 异步请求/响应网络I/O的网络客户机。这是一个内部类，用于实现面向用户的生产者和客户机。
 * <p>
 * This class is not thread-safe!
 */
public class NetworkClient implements KafkaClient {

    private final Logger log;

    /* 这个selector别用来处理高性能的网络io */
    private final Selectable selector;

    private final MetadataUpdater metadataUpdater;

    private final Random randOffset;

    /* the state of each node's connection */
    private final ClusterConnectionStates connectionStates;

    /* 当前正在发送或等待响应的一组请求。  */
    private final InFlightRequests inFlightRequests;

    /* 发送的缓冲区大小 */
    private final int socketSendBuffer;

    /* socket接受的缓冲区大小 */
    private final int socketReceiveBuffer;

    /* the client id used to identify this client in requests to the server */
    private final String clientId;

    /* 向服务器发送请求时使用的当前相关ID  */
    private int correlation;

    /* 生产者等待服务器确认的最大毫秒时间 */
    private final int requestTimeoutMs;

    /* time in ms to wait before retrying to create connection to a server */
    private final long reconnectBackoffMs;

    private final Time time;

    /**
     * 如果为true，则在连接完成后马上发送ApiVersionRequest接口
     */
    private final boolean discoverBrokerVersions;

    private final ApiVersions apiVersions;

    private final Map<String, ApiVersionsRequest.Builder> nodesNeedingApiVersionsFetch = new HashMap<>();

    private final List<ClientResponse> abortedSends = new LinkedList<>();

    private final Sensor throttleTimeSensor;

    public NetworkClient(Selectable selector,
                         Metadata metadata,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         long reconnectBackoffMax,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int requestTimeoutMs,
                         Time time,
                         boolean discoverBrokerVersions,
                         ApiVersions apiVersions,
                         LogContext logContext) {
        this(null, metadata, selector, clientId, maxInFlightRequestsPerConnection,
             reconnectBackoffMs, reconnectBackoffMax,
             socketSendBuffer, socketReceiveBuffer, requestTimeoutMs, time,
             discoverBrokerVersions, apiVersions, null, logContext);
    }

    public NetworkClient(Selectable selector,
            Metadata metadata,
            String clientId,
            int maxInFlightRequestsPerConnection,
            long reconnectBackoffMs,
            long reconnectBackoffMax,
            int socketSendBuffer,
            int socketReceiveBuffer,
            int requestTimeoutMs,
            Time time,
            boolean discoverBrokerVersions,
            ApiVersions apiVersions,
            Sensor throttleTimeSensor,
            LogContext logContext) {
        this(null, metadata, selector, clientId, maxInFlightRequestsPerConnection,
             reconnectBackoffMs, reconnectBackoffMax,
             socketSendBuffer, socketReceiveBuffer, requestTimeoutMs, time,
             discoverBrokerVersions, apiVersions, throttleTimeSensor, logContext);
    }

    public NetworkClient(Selectable selector,
                         MetadataUpdater metadataUpdater,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         long reconnectBackoffMax,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int requestTimeoutMs,
                         Time time,
                         boolean discoverBrokerVersions,
                         ApiVersions apiVersions,
                         LogContext logContext) {
        this(metadataUpdater, null, selector, clientId, maxInFlightRequestsPerConnection,
             reconnectBackoffMs, reconnectBackoffMax,
             socketSendBuffer, socketReceiveBuffer, requestTimeoutMs, time,
             discoverBrokerVersions, apiVersions, null, logContext);
    }

    private NetworkClient(MetadataUpdater metadataUpdater,
                          Metadata metadata,
                          Selectable selector,
                          String clientId,
                          int maxInFlightRequestsPerConnection,
                          long reconnectBackoffMs,
                          long reconnectBackoffMax,
                          int socketSendBuffer,
                          int socketReceiveBuffer,
                          int requestTimeoutMs,
                          Time time,
                          boolean discoverBrokerVersions,
                          ApiVersions apiVersions,
                          Sensor throttleTimeSensor,
                          LogContext logContext) {
        /* It would be better if we could pass `DefaultMetadataUpdater` from the public constructor, but it's not
         * possible because `DefaultMetadataUpdater` is an inner class and it can only be instantiated after the
         * super constructor is invoked.
         */
        if (metadataUpdater == null) {
            if (metadata == null)
                throw new IllegalArgumentException("`metadata` must not be null");
            this.metadataUpdater = new DefaultMetadataUpdater(metadata);
        } else {
            this.metadataUpdater = metadataUpdater;
        }
        this.selector = selector;
        this.clientId = clientId;
        this.inFlightRequests = new InFlightRequests(maxInFlightRequestsPerConnection);
        this.connectionStates = new ClusterConnectionStates(reconnectBackoffMs, reconnectBackoffMax);
        this.socketSendBuffer = socketSendBuffer;
        this.socketReceiveBuffer = socketReceiveBuffer;
        this.correlation = 0;
        this.randOffset = new Random();
        this.requestTimeoutMs = requestTimeoutMs;
        this.reconnectBackoffMs = reconnectBackoffMs;
        this.time = time;
        this.discoverBrokerVersions = discoverBrokerVersions;
        this.apiVersions = apiVersions;
        this.throttleTimeSensor = throttleTimeSensor;
        this.log = logContext.logger(NetworkClient.class);
    }

    /**
     * 开始连接到给定节点，如果我们已经连接并准备发送到该节点，则返回true。
     *
     * @param node The node to check
     * @param now The current timestamp
     * @return True if we are ready to send to the given node
     */
    @Override
    public boolean ready(Node node, long now) {
        if (node.isEmpty())
            throw new IllegalArgumentException("Cannot connect to empty node " + node);

        if (isReady(node, now))
            return true;

        if (connectionStates.canConnect(node.idString(), now))
            // 如果我们有兴趣发送到一个节点并且我们没有连接到它，请启动一个节点
            initiateConnect(node, now);

        return false;
    }

    // Visible for testing
    boolean canConnect(Node node, long now) {
        return connectionStates.canConnect(node.idString(), now);
    }

    /**
     * 断开连接到一个特定的节点，如果有一个。
     * 任何等待客户端请求此连接将断开。
     *
     * @param nodeId The id of the node
     */
    @Override
    public void disconnect(String nodeId) {
        if (connectionStates.isDisconnected(nodeId))
            return;

        selector.close(nodeId);
        List<ApiKeys> requestTypes = new ArrayList<>();
        long now = time.milliseconds();
        for (InFlightRequest request : inFlightRequests.clearAll(nodeId)) {
            if (request.isInternalRequest) {// 内部请求
                if (request.header.apiKey() == ApiKeys.METADATA) {
                    metadataUpdater.handleDisconnection(request.destination);
                }
            } else {// 外部请求，进入到abortedSends 队列中
                requestTypes.add(request.header.apiKey());
                abortedSends.add(new ClientResponse(request.header,
                        request.callback, request.destination, request.createdTimeMs, now,
                        true, null, null));
            }
        }
        connectionStates.disconnected(nodeId, now);
        if (log.isDebugEnabled()) {
            log.debug("Manually disconnected from {}. Removed requests: {}.", nodeId,
                Utils.join(requestTypes, ", "));
        }
    }

    /**
     * Closes the connection to a particular node (if there is one).
     * All requests on the connection will be cleared.  ClientRequest callbacks will not be invoked
     * for the cleared requests, nor will they be returned from poll().
     *
     * @param nodeId The id of the node
     */
    @Override
    public void close(String nodeId) {
        selector.close(nodeId);
        for (InFlightRequest request : inFlightRequests.clearAll(nodeId))
            if (request.isInternalRequest && request.header.apiKey() == ApiKeys.METADATA)
                metadataUpdater.handleDisconnection(request.destination);
        connectionStates.remove(nodeId);
    }

    /**
     * Returns the number of milliseconds to wait, based on the connection state, before attempting to send data. When
     * disconnected, this respects the reconnect backoff time. When connecting or connected, this handles slow/stalled
     * connections.
     *
     * @param node The node to check
     * @param now The current timestamp
     * @return The number of milliseconds to wait.
     */
    @Override
    public long connectionDelay(Node node, long now) {
        return connectionStates.connectionDelay(node.idString(), now);
    }

    /**
     * Check if the connection of the node has failed, based on the connection state. Such connection failure are
     * usually transient and can be resumed in the next {@link #ready(org.apache.kafka.common.Node, long)} }
     * call, but there are cases where transient failures needs to be caught and re-acted upon.
     *
     * @param node the node to check
     * @return true iff the connection has failed and the node is disconnected
     */
    @Override
    public boolean connectionFailed(Node node) {
        return connectionStates.isDisconnected(node.idString());
    }

    /**
     * Check if authentication to this node has failed, based on the connection state. Authentication failures are
     * propagated without any retries.
     *
     * @param node the node to check
     * @return an AuthenticationException iff authentication has failed, null otherwise
     */
    @Override
    public AuthenticationException authenticationException(Node node) {
        return connectionStates.authenticationException(node.idString());
    }

    /**
     * 检查给定ID的节点是否准备发送更多请求。
     *
     * @param node The node
     * @param now The current time in ms
     * @return true if the node is ready
     */
    @Override
    public boolean isReady(Node node, long now) {
        // if we need to update our metadata now declare all requests unready to make metadata requests first
        // priority
        return !metadataUpdater.isUpdateDue(now) && canSendRequest(node.idString());
    }

    /**
     * 连接和准备好了，并且能够向给定的连接发送更多的请求吗？
     *
     * @param node The node
     */
    private boolean canSendRequest(String node) {
        return connectionStates.isReady(node) && selector.isChannelReady(node) && inFlightRequests.canSendMore(node);
    }

    /**
     * Queue up the given request for sending. Requests can only be sent out to ready nodes.
     * @param request The request
     * @param now The current timestamp
     */
    @Override
    public void send(ClientRequest request, long now) {
        doSend(request, false, now);
    }

    private void sendInternalMetadataRequest(MetadataRequest.Builder builder,
                                             String nodeConnectionId, long now) {
        ClientRequest clientRequest = newClientRequest(nodeConnectionId, builder, now, true);
        doSend(clientRequest, true, now);
    }

    /**
     * 发送请求
     * @param clientRequest
     * @param isInternalRequest 内部发送不需要验证连接状态
     * @param now
     */
    private void doSend(ClientRequest clientRequest, boolean isInternalRequest, long now) {
        String nodeId = clientRequest.destination();
        if (!isInternalRequest) {
            // If this request came from outside the NetworkClient, validate
            // that we can send data.  If the request is internal, we trust
            // that internal code has done this validation.  Validation
            // will be slightly different for some internal requests (for
            // example, ApiVersionsRequests can be sent prior to being in
            // READY state.)
            if (!canSendRequest(nodeId))
                throw new IllegalStateException("Attempt to send a request to node " + nodeId + " which is not ready.");
        }
        AbstractRequest.Builder<?> builder = clientRequest.requestBuilder();
        try {
            NodeApiVersions versionInfo = apiVersions.get(nodeId);
            short version;
            // Note: if versionInfo is null, we have no server version information. This would be
            // the case when sending the initial ApiVersionRequest which fetches the version
            // information itself.  It is also the case when discoverBrokerVersions is set to false.
            if (versionInfo == null) {
                version = builder.latestAllowedVersion();
                if (discoverBrokerVersions && log.isTraceEnabled())
                    log.trace("No version information found when sending {} with correlation id {} to node {}. " +
                            "Assuming version {}.", clientRequest.apiKey(), clientRequest.correlationId(), nodeId, version);
            } else {
                version = versionInfo.latestUsableVersion(clientRequest.apiKey(), builder.oldestAllowedVersion(),
                        builder.latestAllowedVersion());
            }
            // The call to build may also throw UnsupportedVersionException, if there are essential
            // fields that cannot be represented in the chosen version.
            doSend(clientRequest, isInternalRequest, now, builder.build(version));// 将请求实体进行发送（根据版本后的）
        } catch (UnsupportedVersionException e) {
            // If the version is not supported, skip sending the request over the wire.
            // Instead, simply add it to the local queue of aborted requests.
            log.debug("Version mismatch when attempting to send {} with correlation id {} to {}", builder,
                    clientRequest.correlationId(), clientRequest.destination(), e);
            ClientResponse clientResponse = new ClientResponse(clientRequest.makeHeader(builder.latestAllowedVersion()),
                    clientRequest.callback(), clientRequest.destination(), now, now,
                    false, e, null);
            abortedSends.add(clientResponse);
        }
    }

    private void doSend(ClientRequest clientRequest, boolean isInternalRequest, long now, AbstractRequest request) {
        String nodeId = clientRequest.destination();
        RequestHeader header = clientRequest.makeHeader(request.version());// 获取请求的头信息，用于序列化
        if (log.isDebugEnabled()) {
            int latestClientVersion = clientRequest.apiKey().latestVersion();
            if (header.apiVersion() == latestClientVersion) {
                log.trace("Sending {} {} with correlation id {} to node {}", clientRequest.apiKey(), request,
                        clientRequest.correlationId(), nodeId);
            } else {
                log.debug("Using older server API v{} to send {} {} with correlation id {} to node {}",
                        header.apiVersion(), clientRequest.apiKey(), request, clientRequest.correlationId(), nodeId);
            }
        }
        Send send = request.toSend(nodeId, header);// 装换为 NetworkSend
        InFlightRequest inFlightRequest = new InFlightRequest(
                header,
                clientRequest.createdTimeMs(),
                clientRequest.destination(),
                clientRequest.callback(),
                clientRequest.expectResponse(),
                isInternalRequest,
                request,
                send,
                now);
        this.inFlightRequests.add(inFlightRequest);
        selector.send(inFlightRequest.send);
    }

    /**
     * 对套接字执行实际读写操作。
     *
     * @param timeout The maximum amount of time to wait (in ms) for responses if there are none immediately,
     *                must be non-negative. The actual timeout will be the minimum of timeout, request timeout and
     *                metadata timeout
     * @param now The current time in milliseconds
     * @return The list of responses received
     */
    @Override
    public List<ClientResponse> poll(long timeout, long now) {
        if (!abortedSends.isEmpty()) {// 处理中断的请求
            // If there are aborted sends because of unsupported version exceptions or disconnects,
            // handle them immediately without waiting for Selector#poll.
            List<ClientResponse> responses = new ArrayList<>();
            handleAbortedSends(responses);
            completeResponses(responses);
            return responses;
        }

        long metadataTimeout = metadataUpdater.maybeUpdate(now);
        try {
            this.selector.poll(Utils.min(timeout, metadataTimeout, requestTimeoutMs));
        } catch (IOException e) {
            log.error("Unexpected error during I/O", e);
        }

        // 处理完成动作
        long updatedNow = this.time.milliseconds();
        List<ClientResponse> responses = new ArrayList<>();
        handleCompletedSends(responses, updatedNow);
        handleCompletedReceives(responses, updatedNow);
        handleDisconnections(responses, updatedNow);
        handleConnections();
        handleInitiateApiVersionRequests(updatedNow);
        handleTimedOutRequests(responses, updatedNow);
        completeResponses(responses);

        return responses;
    }

    /**
     * 调用响应的完成回调
     * @param responses
     */
    private void completeResponses(List<ClientResponse> responses) {
        for (ClientResponse response : responses) {
            try {
                response.onComplete();
            } catch (Exception e) {
                log.error("Uncaught error in request completion:", e);
            }
        }
    }

    /**
     * Get the number of in-flight requests
     */
    @Override
    public int inFlightRequestCount() {
        return this.inFlightRequests.count();
    }

    @Override
    public boolean hasInFlightRequests() {
        return !this.inFlightRequests.isEmpty();
    }

    /**
     * Get the number of in-flight requests for a given node
     */
    @Override
    public int inFlightRequestCount(String node) {
        return this.inFlightRequests.count(node);
    }

    @Override
    public boolean hasInFlightRequests(String node) {
        return !this.inFlightRequests.isEmpty(node);
    }

    @Override
    public boolean hasReadyNodes() {
        return connectionStates.hasReadyNodes();
    }

    /**
     * Interrupt the client if it is blocked waiting on I/O.
     */
    @Override
    public void wakeup() {
        this.selector.wakeup();
    }

    /**
     * Close the network client
     */
    @Override
    public void close() {
        this.selector.close();
    }

    /**
     * Choose the node with the fewest outstanding requests which is at least eligible for connection. This method will
     * prefer a node with an existing connection, but will potentially choose a node for which we don't yet have a
     * connection if all existing connections are in use. This method will never choose a node for which there is no
     * existing connection and from which we have disconnected within the reconnect backoff period.
     *
     * @return 请求最少的节点。
     */
    @Override
    public Node leastLoadedNode(long now) {
        List<Node> nodes = this.metadataUpdater.fetchNodes();
        int inflight = Integer.MAX_VALUE;
        Node found = null;

        int offset = this.randOffset.nextInt(nodes.size());
        for (int i = 0; i < nodes.size(); i++) {
            int idx = (offset + i) % nodes.size();
            Node node = nodes.get(idx);
            int currInflight = this.inFlightRequests.count(node.idString());
            if (currInflight == 0 && isReady(node, now)) {
                // if we find an established connection with no in-flight requests we can stop right away
                log.trace("Found least loaded node {} connected with no in-flight requests", node);
                return node;
            } else if (!this.connectionStates.isBlackedOut(node.idString(), now) && currInflight < inflight) {
                // otherwise if this is the best we have found so far, record that
                inflight = currInflight;
                found = node;
            } else if (log.isTraceEnabled()) {
                log.trace("Removing node {} from least loaded node selection: is-blacked-out: {}, in-flight-requests: {}",
                        node, this.connectionStates.isBlackedOut(node.idString(), now), currInflight);
            }
        }

        if (found != null)
            log.trace("Found least loaded node {}", found);
        else
            log.trace("Least loaded node selection failed to find an available node");

        return found;
    }

    public static AbstractResponse parseResponse(ByteBuffer responseBuffer, RequestHeader requestHeader) {
        Struct responseStruct = parseStructMaybeUpdateThrottleTimeMetrics(responseBuffer, requestHeader, null, 0);
        return AbstractResponse.parseResponse(requestHeader.apiKey(), responseStruct);
    }

    /**
     * 根据请求头，将响应实体解析为 Struct
     * @param responseBuffer
     * @param requestHeader
     * @param throttleTimeSensor
     * @param now
     * @return
     */
    private static Struct parseStructMaybeUpdateThrottleTimeMetrics(ByteBuffer responseBuffer, RequestHeader requestHeader,
                                                                    Sensor throttleTimeSensor, long now) {
        ResponseHeader responseHeader = ResponseHeader.parse(responseBuffer);// 解析响应头
        // Always expect the response version id to be the same as the request version id
        Struct responseBody = requestHeader.apiKey().parseResponse(requestHeader.apiVersion(), responseBuffer);
        correlate(requestHeader, responseHeader);// 验证相关性Id
        if (throttleTimeSensor != null && responseBody.hasField(CommonFields.THROTTLE_TIME_MS))
            throttleTimeSensor.record(responseBody.get(CommonFields.THROTTLE_TIME_MS), now);
        return responseBody;
    }

    /**
     * 节点的处理断开
     *
     * @param responses The list of responses to update
     * @param nodeId Id of the node to be disconnected
     * @param now The current time
     */
    private void processDisconnection(List<ClientResponse> responses, String nodeId, long now, ChannelState disconnectState) {
        connectionStates.disconnected(nodeId, now);
        apiVersions.remove(nodeId);
        nodesNeedingApiVersionsFetch.remove(nodeId); // 移除指定节点的api请求
        switch (disconnectState.state()) {// 记录连接状态
            case AUTHENTICATION_FAILED:
                connectionStates.authenticationFailed(nodeId, now, disconnectState.exception());
                log.error("Connection to node {} failed authentication due to: {}", nodeId, disconnectState.exception().getMessage());
                break;
            case AUTHENTICATE:
                // This warning applies to older brokers which dont provide feedback on authentication failures
                log.warn("Connection to node {} terminated during authentication. This may indicate " +
                        "that authentication failed due to invalid credentials.", nodeId);
                break;
            case NOT_CONNECTED:
                log.warn("Connection to node {} could not be established. Broker may not be available.", nodeId);
                break;
            default:
                break; // Disconnections in other states are logged at debug level in Selector
        }
        for (InFlightRequest request : this.inFlightRequests.clearAll(nodeId)) {
            log.trace("Cancelled request {} {} with correlation id {} due to node {} being disconnected",
                    request.header.apiKey(), request.request, request.header.correlationId(), nodeId);
            if (!request.isInternalRequest)
                responses.add(request.disconnected(now));
            else if (request.header.apiKey() == ApiKeys.METADATA)
                metadataUpdater.handleDisconnection(request.destination);
        }
        AuthenticationException authenticationException = connectionStates.authenticationException(nodeId);
        if (authenticationException != null)
            metadataUpdater.handleAuthenticationFailure(authenticationException);
    }

    /**
     * Iterate over all the inflight requests and expire any requests that have exceeded the configured requestTimeout.
     * The connection to the node associated with the request will be terminated and will be treated as a disconnection.
     *
     * @param responses The list of responses to update
     * @param now The current time
     */
    private void handleTimedOutRequests(List<ClientResponse> responses, long now) {
        List<String> nodeIds = this.inFlightRequests.getNodesWithTimedOutRequests(now, this.requestTimeoutMs);
        for (String nodeId : nodeIds) {
            // 关闭连接节点
            this.selector.close(nodeId);
            log.debug("Disconnecting from node {} due to request timeout.", nodeId);
            processDisconnection(responses, nodeId, now, ChannelState.LOCAL_CLOSE);
        }

        // 我们断开连接，因此我们应该刷新元数据。
        if (!nodeIds.isEmpty())
            metadataUpdater.requestUpdate();
    }

    /**
     * 处理中断请求
     * @param responses
     */
    private void handleAbortedSends(List<ClientResponse> responses) {
        responses.addAll(abortedSends);
        abortedSends.clear();
    }

    /**
     * 处理所有已完成的请求发送。特别是如果没有响应，请考虑请求完成。
     *
     * @param responses The list of responses to update
     * @param now The current time
     */
    private void handleCompletedSends(List<ClientResponse> responses, long now) {
        // if no response is expected then when the send is completed, return it
        for (Send send : this.selector.completedSends()) {
            InFlightRequest request = this.inFlightRequests.lastSent(send.destination());
            if (!request.expectResponse) {
                this.inFlightRequests.completeLastSent(send.destination());
                responses.add(request.completed(null, now));
            }
        }
    }

    /**
     * 处理所有已完成的接收，并用接收到的响应更新响应列表。
     *
     * @param responses The list of responses to update
     * @param now The current time
     */
    private void handleCompletedReceives(List<ClientResponse> responses, long now) {
        for (NetworkReceive receive : this.selector.completedReceives()) {// 完成接受的消息
            String source = receive.source();
            InFlightRequest req = inFlightRequests.completeNext(source);// 最新发送的请求
            Struct responseStruct = parseStructMaybeUpdateThrottleTimeMetrics(receive.payload(), req.header,
                throttleTimeSensor, now);
            if (log.isTraceEnabled()) {
                log.trace("Completed receive from node {} for {} with correlation id {}, received {}", req.destination,
                    req.header.apiKey(), req.header.correlationId(), responseStruct);
            }
            AbstractResponse body = AbstractResponse.parseResponse(req.header.apiKey(), responseStruct);// 请求的响应
            if (req.isInternalRequest && body instanceof MetadataResponse)// 元素响应，更新元数据
                metadataUpdater.handleCompletedMetadataResponse(req.header, now, (MetadataResponse) body);
            else if (req.isInternalRequest && body instanceof ApiVersionsResponse)// api的版本响应
                handleApiVersionsResponse(responses, req, now, (ApiVersionsResponse) body);
            else
                responses.add(req.completed(body, now));
        }
    }

    // 处理api请求的响应
    private void handleApiVersionsResponse(List<ClientResponse> responses,
                                           InFlightRequest req, long now, ApiVersionsResponse apiVersionsResponse) {
        final String node = req.destination;
        if (apiVersionsResponse.error() != Errors.NONE) {
            if (req.request.version() == 0 || apiVersionsResponse.error() != Errors.UNSUPPORTED_VERSION) {// 不支持的版本
                log.warn("Received error {} from node {} when making an ApiVersionsRequest with correlation id {}. Disconnecting.",
                        apiVersionsResponse.error(), node, req.header.correlationId());
                this.selector.close(node);// 关闭选择器的指定通道
                processDisconnection(responses, node, now, ChannelState.LOCAL_CLOSE); // 本地关闭
            } else {
                nodesNeedingApiVersionsFetch.put(node, new ApiVersionsRequest.Builder((short) 0)); // 不支持的api版本请求，缩小版本
            }
            return;
        }
        // 更新节点api版本
        NodeApiVersions nodeVersionInfo = new NodeApiVersions(apiVersionsResponse.apiVersions());
        apiVersions.update(node, nodeVersionInfo);
        this.connectionStates.ready(node);
        if (log.isDebugEnabled()) {
            log.debug("Recorded API versions for node {}: {}", node, nodeVersionInfo);
        }
    }

    /**
     * 处理任何断开连接
     *
     * @param responses The list of responses that completed with the disconnection
     * @param now The current time
     */
    private void handleDisconnections(List<ClientResponse> responses, long now) {
        for (Map.Entry<String, ChannelState> entry : this.selector.disconnected().entrySet()) {
            String node = entry.getKey();
            log.debug("Node {} disconnected.", node);
            processDisconnection(responses, node, now, entry.getValue());
        }
        // we got a disconnect so we should probably refresh our metadata and see if that broker is dead
        if (this.selector.disconnected().size() > 0)
            metadataUpdater.requestUpdate();
    }

    /**
     * Record any newly completed connections
     */
    private void handleConnections() {
        for (String node : this.selector.connected()) {
            // We are now connected.  Node that we might not still be able to send requests. For instance,
            // if SSL is enabled, the SSL handshake happens after the connection is established.
            // Therefore, it is still necessary to check isChannelReady before attempting to send on this
            // connection.
            if (discoverBrokerVersions) {
                this.connectionStates.checkingApiVersions(node);
                nodesNeedingApiVersionsFetch.put(node, new ApiVersionsRequest.Builder());  // 如果连接成功的话，需要添加api请求
                log.debug("Completed connection to node {}. Fetching API versions.", node);
            } else {
                this.connectionStates.ready(node);
                log.debug("Completed connection to node {}. Ready.", node);
            }
        }
    }

    // 处理api初始化请求
    private void handleInitiateApiVersionRequests(long now) {
        Iterator<Map.Entry<String, ApiVersionsRequest.Builder>> iter = nodesNeedingApiVersionsFetch.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, ApiVersionsRequest.Builder> entry = iter.next();
            String node = entry.getKey();
            if (selector.isChannelReady(node) && inFlightRequests.canSendMore(node)) {
                log.debug("Initiating API versions fetch from node {}.", node);
                ApiVersionsRequest.Builder apiVersionRequestBuilder = entry.getValue();
                ClientRequest clientRequest = newClientRequest(node, apiVersionRequestBuilder, now, true);
                doSend(clientRequest, true, now);
                iter.remove();
            }
        }
    }

    /**
     * 验证响应与我们期望的请求对应，否则就会抛出异常
     */
    private static void correlate(RequestHeader requestHeader, ResponseHeader responseHeader) {
        if (requestHeader.correlationId() != responseHeader.correlationId())
            throw new IllegalStateException("Correlation id for response (" + responseHeader.correlationId()
                    + ") does not match request (" + requestHeader.correlationId() + "), request header: " + requestHeader);
    }

    /**
     * Initiate a connection to the given node
     */
    private void initiateConnect(Node node, long now) {
        String nodeConnectionId = node.idString();
        try {
            log.debug("Initiating connection to node {}", node);
            this.connectionStates.connecting(nodeConnectionId, now);
            selector.connect(nodeConnectionId,
                             new InetSocketAddress(node.host(), node.port()),
                             this.socketSendBuffer,
                             this.socketReceiveBuffer);
        } catch (IOException e) {
            /* attempt failed, we'll try again after the backoff */
            connectionStates.disconnected(nodeConnectionId, now);
            /* maybe the problem is our metadata, update it */
            metadataUpdater.requestUpdate();
            log.debug("Error connecting to node {}", node, e);
        }
    }

    class DefaultMetadataUpdater implements MetadataUpdater {

        /* the current cluster metadata */
        private final Metadata metadata;

        /* 有一个元数据请求已经发送，我们还没有收到响应。  */
        private boolean metadataFetchInProgress;

        DefaultMetadataUpdater(Metadata metadata) {
            this.metadata = metadata;
            this.metadataFetchInProgress = false;
        }

        @Override
        public List<Node> fetchNodes() {
            return metadata.fetch().nodes();
        }

        @Override
        public boolean isUpdateDue(long now) {
            return !this.metadataFetchInProgress && this.metadata.timeToNextUpdate(now) == 0;
        }

        @Override
        public long maybeUpdate(long now) {
            // should we update our metadata?
            long timeToNextMetadataUpdate = metadata.timeToNextUpdate(now);
            long waitForMetadataFetch = this.metadataFetchInProgress ? requestTimeoutMs : 0;

            long metadataTimeout = Math.max(timeToNextMetadataUpdate, waitForMetadataFetch);
            if (metadataTimeout > 0) {// 如果元素过期时间大于0，则返回更新过期时间
                return metadataTimeout;
            }

            // Beware that the behavior of this method and the computation of timeouts for poll() are
            // highly dependent on the behavior of leastLoadedNode.
            Node node = leastLoadedNode(now);// 获取待接收请求数目最少
            if (node == null) {
                log.debug("Give up sending metadata request since no node is available");
                return reconnectBackoffMs;
            }

            return maybeUpdate(now, node);
        }

        @Override
        public void handleDisconnection(String destination) {
            Cluster cluster = metadata.fetch();
            // 'processDisconnection' generates warnings for misconfigured bootstrap server configuration
            // resulting in 'Connection Refused' and misconfigured security resulting in authentication failures.
            // The warning below handles the case where connection to a broker was established, but was disconnected
            // before metadata could be obtained.
            if (cluster.isBootstrapConfigured()) {
                int nodeId = Integer.parseInt(destination);
                Node node = cluster.nodeById(nodeId);
                if (node != null)
                    log.warn("Bootstrap broker {} disconnected", node);
            }

            metadataFetchInProgress = false;
        }

        @Override
        public void handleAuthenticationFailure(AuthenticationException exception) {
            metadataFetchInProgress = false;
            if (metadata.updateRequested())
                metadata.failedUpdate(time.milliseconds(), exception);
        }

        // 处理完成元数据请求
        @Override
        public void handleCompletedMetadataResponse(RequestHeader requestHeader, long now, MetadataResponse response) {
            this.metadataFetchInProgress = false;
            Cluster cluster = response.cluster();
            // check if any topics metadata failed to get updated
            Map<String, Errors> errors = response.errors();
            if (!errors.isEmpty())
                log.warn("Error while fetching metadata with correlation id {} : {}", requestHeader.correlationId(), errors);

            // don't update the cluster if there are no valid nodes...the topic we want may still be in the process of being
            // created which means we will get errors and no nodes until it exists
            if (cluster.nodes().size() > 0) { // 成功更新
                this.metadata.update(cluster, response.unavailableTopics(), now);
            } else { // 失败更新
                log.trace("Ignoring empty metadata response with correlation id {}.", requestHeader.correlationId());
                this.metadata.failedUpdate(now, null);
            }
        }

        @Override
        public void requestUpdate() {
            this.metadata.requestUpdate();
        }

        /**
         * Return true if there's at least one connection establishment is currently underway
         */
        private boolean isAnyNodeConnecting() {
            for (Node node : fetchNodes()) {
                if (connectionStates.isConnecting(node.idString())) {
                    return true;
                }
            }
            return false;
        }

        /**
         * 如果我们可以发送一个元数据请求到发送列表中
         */
        private long maybeUpdate(long now, Node node) {
            String nodeConnectionId = node.idString();

            if (canSendRequest(nodeConnectionId)) {// 发送一个元数据请求
                this.metadataFetchInProgress = true;
                MetadataRequest.Builder metadataRequest;
                if (metadata.needMetadataForAllTopics())
                    metadataRequest = MetadataRequest.Builder.allTopics();
                else
                    metadataRequest = new MetadataRequest.Builder(new ArrayList<>(metadata.topics()),
                            metadata.allowAutoTopicCreation());


                log.debug("Sending metadata request {} to node {}", metadataRequest, node);
                sendInternalMetadataRequest(metadataRequest, nodeConnectionId, now);
                return requestTimeoutMs;
            }

            // If there's any connection establishment underway, wait until it completes. This prevents
            // the client from unnecessarily connecting to additional nodes while a previous connection
            // attempt has not been completed.
            if (isAnyNodeConnecting()) {// 判断是否有连接中的节点
                // Strictly the timeout we should return here is "connect timeout", but as we don't
                // have such application level configuration, using reconnect backoff instead.
                return reconnectBackoffMs;
            }

            if (connectionStates.canConnect(nodeConnectionId, now)) {// 判断节点是否可以连接
                // we don't have a connection to this node right now, make one
                log.debug("Initialize connection to node {} for sending metadata request", node);
                initiateConnect(node, now);
                return reconnectBackoffMs;
            }

            // 连接，但不能发送更多的OR连接
            // 无论哪种情况，我们只需要等待网络事件让我们知道所选内容即可
            // 连接可能会再次使用。
            return Long.MAX_VALUE;
        }

    }

    /**
     * 创建一个新的 ClientRequest
     * @param nodeId the node to send to
     * @param requestBuilder the request builder to use
     * @param createdTimeMs the time in milliseconds to use as the creation time of the request
     * @param expectResponse 如果为真，我们期待一个响应
     * @return
     */
    @Override
    public ClientRequest newClientRequest(String nodeId, AbstractRequest.Builder<?> requestBuilder, long createdTimeMs,
                                          boolean expectResponse) {
        return newClientRequest(nodeId, requestBuilder, createdTimeMs, expectResponse, null);
    }

    @Override
    public ClientRequest newClientRequest(String nodeId, AbstractRequest.Builder<?> requestBuilder, long createdTimeMs,
                                          boolean expectResponse, RequestCompletionHandler callback) {
        return new ClientRequest(nodeId, requestBuilder, correlation++, clientId, createdTimeMs, expectResponse,
                callback);
    }

    static class InFlightRequest {
        final RequestHeader header;
        final String destination;
        final RequestCompletionHandler callback;
        final boolean expectResponse;
        final AbstractRequest request;
        final boolean isInternalRequest; // used to flag requests which are initiated internally by NetworkClient
        final Send send;
        final long sendTimeMs;
        final long createdTimeMs;

        public InFlightRequest(RequestHeader header,
                               long createdTimeMs,
                               String destination,
                               RequestCompletionHandler callback,
                               boolean expectResponse,
                               boolean isInternalRequest,
                               AbstractRequest request,
                               Send send,
                               long sendTimeMs) {
            this.header = header;
            this.destination = destination;
            this.callback = callback;
            this.expectResponse = expectResponse;
            this.isInternalRequest = isInternalRequest;
            this.request = request;
            this.send = send;
            this.sendTimeMs = sendTimeMs;
            this.createdTimeMs = createdTimeMs;
        }

        /**
         * 构造完成响应
         * @param response
         * @param timeMs
         * @return
         */
        public ClientResponse completed(AbstractResponse response, long timeMs) {
            return new ClientResponse(header, callback, destination, createdTimeMs, timeMs, false, null, response);
        }

        /**
         * 构造断开连接的响应
         * @param timeMs
         * @return
         */
        public ClientResponse disconnected(long timeMs) {
            return new ClientResponse(header, callback, destination, createdTimeMs, timeMs, true, null, null);
        }

        @Override
        public String toString() {
            return "InFlightRequest(header=" + header +
                    ", destination=" + destination +
                    ", expectResponse=" + expectResponse +
                    ", createdTimeMs=" + createdTimeMs +
                    ", sendTimeMs=" + sendTimeMs +
                    ", isInternalRequest=" + isInternalRequest +
                    ", request=" + request +
                    ", callback=" + callback +
                    ", send=" + send + ")";
        }
    }

    public boolean discoverBrokerVersions() {
        return discoverBrokerVersions;
    }
}
