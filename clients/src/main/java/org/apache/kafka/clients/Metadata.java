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
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A class encapsulating some of the logic around metadata.
 * <p>
 * This class is shared by the client thread (for partitioning) and the background sender thread.
 *
 * Metadata is maintained for only a subset of topics, which can be added to over time. When we request metadata for a
 * topic we don't have any metadata for it will trigger a metadata update.
 * <p>
 * If topic expiry is enabled for the metadata, any topic that has not been used within the expiry interval
 * is removed from the metadata refresh set after an update. Consumers disable topic expiry since they explicitly
 * manage topics while producers rely on topic expiry to limit the refresh set.
 */
public final class Metadata {

    private static final Logger log = LoggerFactory.getLogger(Metadata.class);

    public static final long TOPIC_EXPIRY_MS = 5 * 60 * 1000;// 主题的过期时间
    private static final long TOPIC_EXPIRY_NEEDS_UPDATE = -1L;

    private final long refreshBackoffMs;
    private final long metadataExpireMs;
    private int version;
    private long lastRefreshMs;
    private long lastSuccessfulRefreshMs;
    private AuthenticationException authenticationException;
    private Cluster cluster;
    private boolean needUpdate;// 判断是否需要更新元数据
    /* Topics with expiry time */
    private final Map<String, Long> topics; // 主题名和过期时间
    private final List<Listener> listeners;// 元数据的监听器
    private final ClusterResourceListeners clusterResourceListeners; // 集群资源的监听器
    private boolean needMetadataForAllTopics;
    private final boolean allowAutoTopicCreation;
    private final boolean topicExpiryEnabled;

    public Metadata(long refreshBackoffMs, long metadataExpireMs, boolean allowAutoTopicCreation) {
        this(refreshBackoffMs, metadataExpireMs, allowAutoTopicCreation, false, new ClusterResourceListeners());
    }

    /**
     * 创建一个新的元数据实例
     * @param refreshBackoffMs 元数据之间必须缩短的最小时间刷新，以避免繁忙轮询。
     * @param metadataExpireMs 无需刷新就可以保留元数据的最大时间。
     * @param allowAutoTopicCreation 如果代理配置自动创建主题为true，则在发送元数据请求时，将由代理创建不存在的主题。
     * @param topicExpiryEnabled If true, 启用未使用主题的过期
     * @param clusterResourceListeners 对clusterresourcelisteners将元数据更新列表。
     */
    public Metadata(long refreshBackoffMs, long metadataExpireMs, boolean allowAutoTopicCreation,
                    boolean topicExpiryEnabled, ClusterResourceListeners clusterResourceListeners) {
        this.refreshBackoffMs = refreshBackoffMs;
        this.metadataExpireMs = metadataExpireMs;
        this.allowAutoTopicCreation = allowAutoTopicCreation;
        this.topicExpiryEnabled = topicExpiryEnabled;
        this.lastRefreshMs = 0L;
        this.lastSuccessfulRefreshMs = 0L;
        this.version = 0;
        this.cluster = Cluster.empty();
        this.needUpdate = false;
        this.topics = new HashMap<>();
        this.listeners = new ArrayList<>();
        this.clusterResourceListeners = clusterResourceListeners;
        this.needMetadataForAllTopics = false;
    }

    /**
     * 无阻塞获取当前集群信息
     */
    public synchronized Cluster fetch() {
        return this.cluster;
    }

    /**
     * 添加要在元数据中维护的主题。如果启用了主题过期，则将在下一次更新时重置过期时间。
     */
    public synchronized void add(String topic) {
        Objects.requireNonNull(topic, "topic cannot be null");
        if (topics.put(topic, TOPIC_EXPIRY_NEEDS_UPDATE) == null) {
            requestUpdateForNewTopics();
        }
    }

    /**
     * The next time to update the cluster info is the maximum of the time the current info will expire and the time the
     * current info can be updated (i.e. backoff time has elapsed); If an update has been request then the expiry time
     * is now
     * 判断到下一次更新的时间
     * 下一次更新群集信息是当前信息过期的最大时间和当前信息可以更新的时间（即退避时间已经过去）；
     * 如果请求更新了，则到期时间现在是现在。
     */
    public synchronized long timeToNextUpdate(long nowMs) {
        long timeToExpire = needUpdate ? 0 : Math.max(this.lastSuccessfulRefreshMs + this.metadataExpireMs - nowMs, 0);
        long timeToAllowUpdate = this.lastRefreshMs + this.refreshBackoffMs - nowMs;
        return Math.max(timeToExpire, timeToAllowUpdate);
    }

    /**
     * 请求更新当前集群元数据信息，在更新之前返回当前版本
     */
    public synchronized int requestUpdate() {
        this.needUpdate = true;
        return this.version;
    }

    /**
     * Check whether an update has been explicitly requested.
     * @return true if an update was requested, false otherwise
     */
    public synchronized boolean updateRequested() {
        return this.needUpdate;
    }

    /**
     * If any non-retriable authentication exceptions were encountered during
     * metadata update, clear and return the exception.
     */
    public synchronized AuthenticationException getAndClearAuthenticationException() {
        if (authenticationException != null) {
            AuthenticationException exception = authenticationException;
            authenticationException = null;
            return exception;
        } else
            return null;
    }

    /**
     * 等待元数据更新，直到当前版本比我们所知道的最后一个版本大。（异步等待）
     */
    public synchronized void awaitUpdate(final int lastVersion, final long maxWaitMs) throws InterruptedException {
        if (maxWaitMs < 0) {
            throw new IllegalArgumentException("Max time to wait for metadata updates should not be < 0 milliseconds");
        }
        long begin = System.currentTimeMillis();
        long remainingWaitMs = maxWaitMs;// 单次持续等待时间
        while (this.version <= lastVersion) {
            AuthenticationException ex = getAndClearAuthenticationException(); // 如果授权失败，则抛出异常，避免重试
            if (ex != null)
                throw ex;
            if (remainingWaitMs != 0)
                wait(remainingWaitMs); // 等待元数据更新
            long elapsed = System.currentTimeMillis() - begin;
            if (elapsed >= maxWaitMs)
                throw new TimeoutException("Failed to update metadata after " + maxWaitMs + " ms.");
            remainingWaitMs = maxWaitMs - elapsed;
        }
    }

    /**
     * Replace the current set of topics maintained to the one provided.
     * If topic expiry is enabled, expiry time of the topics will be
     * reset on the next update.
     * @param topics
     */
    public synchronized void setTopics(Collection<String> topics) {
        if (!this.topics.keySet().containsAll(topics)) {
            requestUpdateForNewTopics();
        }
        this.topics.clear();
        for (String topic : topics)
            this.topics.put(topic, TOPIC_EXPIRY_NEEDS_UPDATE);
    }

    /**
     * Get the list of topics we are currently maintaining metadata for
     */
    public synchronized Set<String> topics() {
        return new HashSet<>(this.topics.keySet());
    }

    /**
     * Check if a topic is already in the topic set.
     * @param topic topic to check
     * @return true if the topic exists, false otherwise
     */
    public synchronized boolean containsTopic(String topic) {
        return this.topics.containsKey(topic);
    }

    /**
     * 更新集群元数据，
     * 如果启用了主题过期，则如果需要，则为主题设置过期时间，并从元数据中删除过期主题。
     *
     * @param newCluster the cluster containing metadata for topics with valid metadata
     * @param unavailableTopics 不存在或有一个或多个不知道其领导人的分区的主题
     * @param now current time in milliseconds
     */
    public synchronized void update(Cluster newCluster, Set<String> unavailableTopics, long now) {
        Objects.requireNonNull(newCluster, "cluster should not be null");

        this.needUpdate = false;
        this.lastRefreshMs = now;
        this.lastSuccessfulRefreshMs = now;
        this.version += 1;

        if (topicExpiryEnabled) {// 如果开启主题过期时间
            // Handle expiry of topics from the metadata refresh set.
            for (Iterator<Map.Entry<String, Long>> it = topics.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<String, Long> entry = it.next();
                long expireMs = entry.getValue();
                if (expireMs == TOPIC_EXPIRY_NEEDS_UPDATE)
                    entry.setValue(now + TOPIC_EXPIRY_MS);// 设置默认过期时间
                else if (expireMs <= now) {
                    it.remove();
                    log.debug("Removing unused topic {} from the metadata list, expiryMs {} now {}", entry.getKey(), expireMs, now);
                }
            }
        }

        for (Listener listener: listeners)
            listener.onMetadataUpdate(newCluster, unavailableTopics);

        String previousClusterId = cluster.clusterResource().clusterId();

        if (this.needMetadataForAllTopics) {
            // the listener may change the interested topics, which could cause another metadata refresh.
            // If we have already fetched all topics, however, another fetch should be unnecessary.
            this.needUpdate = false;
            this.cluster = getClusterForCurrentTopics(newCluster);
        } else {
            this.cluster = newCluster;
        }

        // 保证自举群集没有任何有用的信息。
        if (!newCluster.isBootstrapConfigured()) {
            String newClusterId = newCluster.clusterResource().clusterId();
            if (newClusterId == null ? previousClusterId != null : !newClusterId.equals(previousClusterId))
                log.info("Cluster ID: {}", newClusterId);
            clusterResourceListeners.onUpdate(newCluster.clusterResource());
        }

        notifyAll();
        log.debug("Updated cluster metadata version {} to {}", this.version, this.cluster);
    }

    /**
     * 记录尝试更新失败的元数据。 我们需要跟踪这一点，以避免立即重试。
     */
    public synchronized void failedUpdate(long now, AuthenticationException authenticationException) {
        this.lastRefreshMs = now;
        this.authenticationException = authenticationException;
        if (authenticationException != null)
            this.notifyAll();
    }

    /**
     * @return The current metadata version
     */
    public synchronized int version() {
        return this.version;
    }

    /**
     * The last time metadata was successfully updated.
     */
    public synchronized long lastSuccessfulUpdate() {
        return this.lastSuccessfulRefreshMs;
    }

    public boolean allowAutoTopicCreation() {
        return allowAutoTopicCreation;
    }

    /**
     * 置状态以指示是否需要卡夫卡集群中所有主题的元数据。
     * @param needMetadataForAllTopics boolean indicating need for metadata of all topics in cluster.
     */
    public synchronized void needMetadataForAllTopics(boolean needMetadataForAllTopics) {
        if (needMetadataForAllTopics && !this.needMetadataForAllTopics) {
            requestUpdateForNewTopics();
        }
        this.needMetadataForAllTopics = needMetadataForAllTopics;
    }

    /**
     * 获取是否需要所有主题的元数据
     */
    public synchronized boolean needMetadataForAllTopics() {
        return this.needMetadataForAllTopics;
    }

    /**
     * Add a Metadata listener that gets notified of metadata updates
     */
    public synchronized void addListener(Listener listener) {
        this.listeners.add(listener);
    }

    /**
     * Stop notifying the listener of metadata updates
     */
    public synchronized void removeListener(Listener listener) {
        this.listeners.remove(listener);
    }

    /**
     * MetadataUpdate Listener
     */
    public interface Listener {
        /**
         * Callback invoked on metadata update.
         *
         * @param cluster the cluster containing metadata for topics with valid metadata
         * @param unavailableTopics topics which are non-existent or have one or more partitions whose
         *        leader is not known
         */
        void onMetadataUpdate(Cluster cluster, Set<String> unavailableTopics);
    }

    /**
     * 为了新添加的主题更新请求
     */
    private synchronized void requestUpdateForNewTopics() {
        // Override the timestamp of last refresh to let immediate update.
        this.lastRefreshMs = 0;
        requestUpdate();
    }

    // 过滤除当前主题的之外的集群
    private Cluster getClusterForCurrentTopics(Cluster cluster) {
        Set<String> unauthorizedTopics = new HashSet<>();
        Collection<PartitionInfo> partitionInfos = new ArrayList<>();
        List<Node> nodes = Collections.emptyList();
        Set<String> internalTopics = Collections.emptySet();
        Node controller = null;
        String clusterId = null;
        if (cluster != null) {
            clusterId = cluster.clusterResource().clusterId();
            internalTopics = cluster.internalTopics();
            unauthorizedTopics.addAll(cluster.unauthorizedTopics());
            unauthorizedTopics.retainAll(this.topics.keySet());

            for (String topic : this.topics.keySet()) {  // 当前的主题
                List<PartitionInfo> partitionInfoList = cluster.partitionsForTopic(topic);
                if (!partitionInfoList.isEmpty()) {
                    partitionInfos.addAll(partitionInfoList);
                }
            }
            nodes = cluster.nodes();
            controller  = cluster.controller();
        }
        return new Cluster(clusterId, nodes, partitionInfos, unauthorizedTopics, internalTopics, controller);
    }
}
