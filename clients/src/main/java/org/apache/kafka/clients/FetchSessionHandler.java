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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchMetadata;
import org.apache.kafka.common.requests.FetchRequest.PartitionData;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static org.apache.kafka.common.requests.FetchMetadata.INVALID_SESSION_ID;

/**
 * FetchSessionHandler 保持获取会话状态连接到broker。
 *
 * 使用KIP-227所概述的协议，客户端可以创建增量获取会话。
 * 这些会话允许客户端反复地获取关于一组分区的信息，而不显式枚举请求和响应中的所有分区。
 *
 * FetchSessionHandler跟踪会话中的分区。
 * 它还确定每个提取请求中需要包含哪些分区，以及每个请求的附加提取会话元数据应该是什么。
 * 接收代理端的相应类是FetchManager。
 */
public class FetchSessionHandler {
    private final Logger log;

    private final int node;

    /**
     * 下一个获取请求的元数据。
     */
    private FetchMetadata nextMetadata = FetchMetadata.INITIAL;

    public FetchSessionHandler(LogContext logContext, int node) {
        this.log = logContext.logger(FetchSessionHandler.class);
        this.node = node;
    }

    /**
     * 提取请求会话中存在的所有分区。
     */
    private LinkedHashMap<TopicPartition, PartitionData> sessionPartitions =
        new LinkedHashMap<>(0);

    public static class FetchRequestData {
        /**
         * 要在获取请求中发送的分区。
         */
        private final Map<TopicPartition, PartitionData> toSend;

        /**
         * 在请求的"forget"列表中发送分区。
         */
        private final List<TopicPartition> toForget;

        /**
         * 取回请求会话中存在的所有分区。
         */
        private final Map<TopicPartition, PartitionData> sessionPartitions;

        /**
         * 在这个获取请求中使用的元数据。
         */
        private final FetchMetadata metadata;

        FetchRequestData(Map<TopicPartition, PartitionData> toSend,
                         List<TopicPartition> toForget,
                         Map<TopicPartition, PartitionData> sessionPartitions,
                         FetchMetadata metadata) {
            this.toSend = toSend;
            this.toForget = toForget;
            this.sessionPartitions = sessionPartitions;
            this.metadata = metadata;
        }

        /**
         * Get the set of partitions to send in this fetch request.
         */
        public Map<TopicPartition, PartitionData> toSend() {
            return toSend;
        }

        /**
         * Get a list of partitions to forget in this fetch request.
         */
        public List<TopicPartition> toForget() {
            return toForget;
        }

        /**
         * 获取此提取请求中涉及的全部分区。
         */
        public Map<TopicPartition, PartitionData> sessionPartitions() {
            return sessionPartitions;
        }

        public FetchMetadata metadata() {
            return metadata;
        }

        @Override
        public String toString() {
            if (metadata.isFull()) {
                StringBuilder bld = new StringBuilder("FullFetchRequest(");
                String prefix = "";
                for (TopicPartition partition : toSend.keySet()) {
                    bld.append(prefix);
                    bld.append(partition);
                    prefix = ", ";
                }
                bld.append(")");
                return bld.toString();
            } else {
                StringBuilder bld = new StringBuilder("IncrementalFetchRequest(toSend=(");
                String prefix = "";
                for (TopicPartition partition : toSend.keySet()) {
                    bld.append(prefix);
                    bld.append(partition);
                    prefix = ", ";
                }
                bld.append("), toForget=(");
                prefix = "";
                for (TopicPartition partition : toForget) {
                    bld.append(prefix);
                    bld.append(partition);
                    prefix = ", ";
                }
                bld.append("), implied=(");
                prefix = "";
                for (TopicPartition partition : sessionPartitions.keySet()) {
                    if (!toSend.containsKey(partition)) {
                        bld.append(prefix);
                        bld.append(partition);
                        prefix = ", ";
                    }
                }
                bld.append("))");
                return bld.toString();
            }
        }
    }

    public class Builder {
        /**
         * 我们想要获取的下一个分区。
         * 通过使用LinkedHashMap而不是常规Map来维护此列表的插入顺序非常重要。
         * 其中一个原因是，在处理完整的提取请求时，如果没有足够的响应空间来从所有分区返回数据，
         * 服务器将仅在此列表的早期返回分区中的数据。
         * 另一个原因是因为我们利用列表排序来优化增量获取请求的准备（见下文）。
         */
        private LinkedHashMap<TopicPartition, PartitionData> next = new LinkedHashMap<>();

        /**
         * Mark that we want data from this partition in the upcoming fetch.
         */
        public void add(TopicPartition topicPartition, PartitionData data) {
            next.put(topicPartition, data);
        }

        // 构建fetch请求数据
        public FetchRequestData build() {
            if (nextMetadata.isFull()) { // 初始化或者完成
                log.debug("Built full fetch {} for node {} with {}.",
                    nextMetadata, node, partitionsToLogString(next.keySet()));
                sessionPartitions = next;
                next = null;
                Map<TopicPartition, PartitionData> toSend =
                    Collections.unmodifiableMap(new LinkedHashMap<>(sessionPartitions));
                return new FetchRequestData(toSend, Collections.<TopicPartition>emptyList(), toSend, nextMetadata);
            }

            List<TopicPartition> added = new ArrayList<>();
            List<TopicPartition> removed = new ArrayList<>();
            List<TopicPartition> altered = new ArrayList<>();
            for (Iterator<Entry<TopicPartition, PartitionData>> iter =
                     sessionPartitions.entrySet().iterator(); iter.hasNext(); ) {
                Entry<TopicPartition, PartitionData> entry = iter.next();
                TopicPartition topicPartition = entry.getKey();
                PartitionData prevData = entry.getValue();// session的
                PartitionData nextData = next.get(topicPartition); // 添加的
                if (nextData != null) {
                    if (prevData.equals(nextData)) {
                        // 从FetchRequest中删除此分区，因为它自上一个请求以来没有更改过。
                        next.remove(topicPartition);
                    } else {
                        // 将已更改的分区移动到“next”的末尾
                        next.remove(topicPartition);
                        next.put(topicPartition, nextData);
                        entry.setValue(nextData);
                        altered.add(topicPartition);
                    }
                } else {
                    // 从session中移除分区
                    iter.remove();
                    // 表明我们不再想监听这个分区。
                    removed.add(topicPartition);
                }
            }
            // 将任何新分区添加到会话中。
            for (Iterator<Entry<TopicPartition, PartitionData>> iter =
                     next.entrySet().iterator(); iter.hasNext(); ) {
                Entry<TopicPartition, PartitionData> entry = iter.next();
                TopicPartition topicPartition = entry.getKey();
                PartitionData nextData = entry.getValue();
                if (sessionPartitions.containsKey(topicPartition)) {
                    // In the previous loop, all the partitions which existed in both sessionPartitions
                    // and next were moved to the end of next, or removed from next.  Therefore,
                    // once we hit one of them, we know there are no more unseen entries to look
                    // at in next.
                    break;
                }
                sessionPartitions.put(topicPartition, nextData);
                added.add(topicPartition); // 添加的分区
            }
            log.debug("Built incremental fetch {} for node {}. Added {}, altered {}, removed {} " +
                    "out of {}", nextMetadata, node, partitionsToLogString(added),
                    partitionsToLogString(altered), partitionsToLogString(removed),
                    partitionsToLogString(sessionPartitions.keySet()));
            Map<TopicPartition, PartitionData> toSend =
                Collections.unmodifiableMap(new LinkedHashMap<>(next));
            Map<TopicPartition, PartitionData> curSessionPartitions =
                Collections.unmodifiableMap(new LinkedHashMap<>(sessionPartitions));
            next = null;
            return new FetchRequestData(toSend, Collections.unmodifiableList(removed),
                curSessionPartitions, nextMetadata);
        }
    }

    public Builder newBuilder() {
        return new Builder();
    }

    private String partitionsToLogString(Collection<TopicPartition> partitions) {
        if (!log.isTraceEnabled()) {
            return String.format("%d partition(s)", partitions.size());
        }
        return "(" + Utils.join(partitions, ", ") + ")";
    }

    /**
     * 返回一些预期会在特定集合中的分区，但不是。
     *
     * @param toFind    The partitions to look for.
     * @param toSearch  The set of partitions to search.
     * @return          null if all partitions were found; some of the missing ones
     *                  in string form, if not.
     */
    static Set<TopicPartition> findMissing(Set<TopicPartition> toFind, Set<TopicPartition> toSearch) {
        Set<TopicPartition> ret = new LinkedHashSet<>();
        for (TopicPartition partition : toFind) {
            if (!toSearch.contains(partition)) {
                ret.add(partition);
            }
        }
        return ret;
    }

    /**
     * 验证完整的提取响应是否包含提取会话中的所有分区。
     *
     * @param response  The response.
     * @return          True if the full fetch response partitions are valid.
     */
    private String verifyFullFetchResponsePartitions(FetchResponse response) {
        StringBuilder bld = new StringBuilder();
        Set<TopicPartition> omitted = // 省略
            findMissing(response.responseData().keySet(), sessionPartitions.keySet());
        Set<TopicPartition> extra = // 额外
            findMissing(sessionPartitions.keySet(), response.responseData().keySet());
        if (!omitted.isEmpty()) {
            bld.append("omitted=(").append(Utils.join(omitted, ", ")).append(", ");
        }
        if (!extra.isEmpty()) {
            bld.append("extra=(").append(Utils.join(extra, ", ")).append(", ");
        }
        if ((!omitted.isEmpty()) || (!extra.isEmpty())) {
            bld.append("response=(").append(Utils.join(response.responseData().keySet(), ", "));
            return bld.toString();
        }
        return null;
    }

    /**
     * 验证会话中是否包含增量提取响应中的分区。
     *
     * @param response  The response.
     * @return          True if the incremental fetch response partitions are valid.
     */
    private String verifyIncrementalFetchResponsePartitions(FetchResponse response) {
        Set<TopicPartition> extra =
            findMissing(response.responseData().keySet(), sessionPartitions.keySet());
        if (!extra.isEmpty()) {
            StringBuilder bld = new StringBuilder();
            bld.append("extra=(").append(Utils.join(extra, ", ")).append("), ");
            bld.append("response=(").append(
                Utils.join(response.responseData().keySet(), ", ")).append("), ");
            return bld.toString();
        }
        return null;
    }

    /**
     * Create a string describing the partitions in a FetchResponse.
     *
     * @param response  The FetchResponse.
     * @return          The string to log.
     */
    private String responseDataToLogString(FetchResponse response) {
        if (!log.isTraceEnabled()) {
            int implied = sessionPartitions.size() - response.responseData().size();
            if (implied > 0) {
                return String.format(" with %d response partition(s), %d implied partition(s)",
                    response.responseData().size(), implied);
            } else {
                return String.format(" with %d response partition(s)",
                    response.responseData().size());
            }
        }
        StringBuilder bld = new StringBuilder();
        bld.append(" with response=(").
            append(Utils.join(response.responseData().keySet(), ", ")).
            append(")");
        String prefix = ", implied=(";
        String suffix = "";
        for (TopicPartition partition : sessionPartitions.keySet()) {
            if (!response.responseData().containsKey(partition)) {
                bld.append(prefix);
                bld.append(partition);
                prefix = ", ";
                suffix = ")";
            }
        }
        bld.append(suffix);
        return bld.toString();
    }

    /**
     * 处理获取响应,主要用于处理nextMetadata
     *
     * @param response  The response.
     * @return          如果答案是完整的，则为真; 如果由于丢失或意外的分区而无法处理，则为false。
     */
    public boolean handleResponse(FetchResponse response) {
        if (response.error() != Errors.NONE) {
            log.info("Node {} was unable to process the fetch request with {}: {}.",
                node, nextMetadata, response.error());
            if (response.error() == Errors.FETCH_SESSION_ID_NOT_FOUND) {
                nextMetadata = FetchMetadata.INITIAL;
            } else {
                nextMetadata = nextMetadata.nextCloseExisting(); // 将epoch初始化为1
            }
            return false;
        } else if (nextMetadata.isFull()) { // 初始化
            String problem = verifyFullFetchResponsePartitions(response);
            if (problem != null) {
                log.info("Node {} sent an invalid full fetch response with {}", node, problem);
                nextMetadata = FetchMetadata.INITIAL;
                return false;
            } else if (response.sessionId() == INVALID_SESSION_ID) {
                log.debug("Node {} sent a full fetch response{}",
                    node, responseDataToLogString(response));
                nextMetadata = FetchMetadata.INITIAL;
                return true;
            } else {
                // 服务器创建了一个新的增量提取会话。
                log.debug("Node {} sent a full fetch response that created a new incremental " +
                    "fetch session {}{}", node, response.sessionId(), responseDataToLogString(response));
                nextMetadata = FetchMetadata.newIncremental(response.sessionId()); // 增加epoch
                return true;
            }
        } else {
            String problem = verifyIncrementalFetchResponsePartitions(response); //
            if (problem != null) {
                log.info("Node {} sent an invalid incremental fetch response with {}", node, problem);
                nextMetadata = nextMetadata.nextCloseExisting();
                return false;
            } else if (response.sessionId() == INVALID_SESSION_ID) {
                // The incremental fetch session was closed by the server.
                log.debug("Node {} sent an incremental fetch response closing session {}{}",
                    node, nextMetadata.sessionId(), responseDataToLogString(response));
                nextMetadata = FetchMetadata.INITIAL;
                return true;
            } else {
                // The incremental fetch session was continued by the server.
                log.debug("Node {} sent an incremental fetch response for session {}{}",
                    node, response.sessionId(), responseDataToLogString(response));
                nextMetadata = nextMetadata.nextIncremental();
                return true;
            }
        }
    }

    /**
     * Handle an error sending the prepared request.
     *
     * When a network error occurs, we close any existing fetch session on our next request,
     * and try to create a new session.
     *
     * @param t     The exception.
     */
    public void handleError(Throwable t) {
        log.info("Error sending fetch request {} to node {}: {}.", nextMetadata, node, t.toString());
        nextMetadata = nextMetadata.nextCloseExisting();
    }
}
