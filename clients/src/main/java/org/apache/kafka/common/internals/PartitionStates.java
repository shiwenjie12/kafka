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
package org.apache.kafka.common.internals;

import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 这个类是一个有用的构建模块，用于执行提取请求，其中主题分区必须通过循环法进行轮换，
 * 以确保公平性和一定程度的确定性，因为存在对提取响应大小的限制。 因为如果将同一主题的所有分区组合在一起，
 * 则获取请求的序列化效率更高，因此我们在`set`方法中进行这样的分组。
 *
 * As partitions are moved to the end, the same topic may be repeated more than once. In the optimal case, a single
 * topic would "wrap around" and appear twice. However, as partitions are fetched in different orders and partition
 * leadership changes, we will deviate from the optimal. If this turns out to be an issue in practice, we can improve
 * it by tracking the partitions per node or calling `set` every so often.
 */
public class PartitionStates<S> {

    private final LinkedHashMap<TopicPartition, S> map = new LinkedHashMap<>();

    public PartitionStates() {}

    // 将制定分区的状态移动到结尾
    public void moveToEnd(TopicPartition topicPartition) {
        S state = map.remove(topicPartition);
        if (state != null)
            map.put(topicPartition, state);
    }

    public void updateAndMoveToEnd(TopicPartition topicPartition, S state) {
        map.remove(topicPartition);
        map.put(topicPartition, state);
    }

    public void remove(TopicPartition topicPartition) {
        map.remove(topicPartition);
    }

    /**
     * Returns the partitions in random order.
     */
    public Set<TopicPartition> partitionSet() {
        return new HashSet<>(map.keySet());
    }

    public void clear() {
        map.clear();
    }

    public boolean contains(TopicPartition topicPartition) {
        return map.containsKey(topicPartition);
    }

    /**
     * 返回排序的分区状态
     */
    public List<PartitionState<S>> partitionStates() {
        List<PartitionState<S>> result = new ArrayList<>();
        for (Map.Entry<TopicPartition, S> entry : map.entrySet()) {
            result.add(new PartitionState<>(entry.getKey(), entry.getValue()));
        }
        return result;
    }

    /**
     * Returns the partition state values in order.
     */
    public List<S> partitionStateValues() {
        return new ArrayList<>(map.values());
    }

    public S stateValue(TopicPartition topicPartition) {
        return map.get(topicPartition);
    }

    public int size() {
        return map.size();
    }

    /**
     * Update the builder to have the received map as its state (i.e. the previous state is cleared). The builder will
     * "batch by topic", so if we have a, b and c, each with two partitions, we may end up with something like the
     * following (the order of topics and partitions within topics is dependent on the iteration order of the received
     * map): a0, a1, b1, b0, c0, c1.
     */
    public void set(Map<TopicPartition, S> partitionToState) {
        map.clear();
        update(partitionToState);
    }

    private void update(Map<TopicPartition, S> partitionToState) {
        LinkedHashMap<String, List<TopicPartition>> topicToPartitions = new LinkedHashMap<>();
        for (TopicPartition tp : partitionToState.keySet()) { // 先将主题进行分组，用于提高效率
            List<TopicPartition> partitions = topicToPartitions.get(tp.topic());
            if (partitions == null) {
                partitions = new ArrayList<>();
                topicToPartitions.put(tp.topic(), partitions);
            }
            partitions.add(tp);
        }
        for (Map.Entry<String, List<TopicPartition>> entry : topicToPartitions.entrySet()) {
            for (TopicPartition tp : entry.getValue()) {
                S state = partitionToState.get(tp);
                map.put(tp, state);
            }
        }
    }

    // 分区上的状态，包含tp和对应的状态
    public static class PartitionState<S> {
        private final TopicPartition topicPartition;
        private final S value;

        public PartitionState(TopicPartition topicPartition, S state) {
            this.topicPartition = Objects.requireNonNull(topicPartition);
            this.value = Objects.requireNonNull(state);
        }

        public S value() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            PartitionState<?> that = (PartitionState<?>) o;

            return topicPartition.equals(that.topicPartition) && value.equals(that.value);
        }

        @Override
        public int hashCode() {
            int result = topicPartition.hashCode();
            result = 31 * result + value.hashCode();
            return result;
        }

        public TopicPartition topicPartition() {
            return topicPartition;
        }

        @Override
        public String toString() {
            return "PartitionState(" + topicPartition + "=" + value + ')';
        }
    }

}
