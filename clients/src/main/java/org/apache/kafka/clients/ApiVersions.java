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

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.ProduceRequest;

import java.util.HashMap;
import java.util.Map;

/**
 * 维护节点的API版本以外的networkclient访问（这是那里的信息来源），模式类似于使用{@link Metadata} 为主题的元数据的元数据。
 * NOTE: 此类仅用于内部使用，仅限于卡夫卡内部。
 */
public class ApiVersions {

    private final Map<String, NodeApiVersions> nodeApiVersions = new HashMap<>();
    private byte maxUsableProduceMagic = RecordBatch.CURRENT_MAGIC_VALUE;

    public synchronized void update(String nodeId, NodeApiVersions nodeApiVersions) {
        this.nodeApiVersions.put(nodeId, nodeApiVersions);
        this.maxUsableProduceMagic = computeMaxUsableProduceMagic();
    }

    public synchronized void remove(String nodeId) {
        this.nodeApiVersions.remove(nodeId);
        this.maxUsableProduceMagic = computeMaxUsableProduceMagic();
    }

    public synchronized NodeApiVersions get(String nodeId) {
        return this.nodeApiVersions.get(nodeId);
    }

    /**
     *计算最大的可用版本 {@link ApiKeys.PRODUCE}
     * @return
     */
    private byte computeMaxUsableProduceMagic() {
        // use a magic version which is supported by all brokers to reduce the chance that
        // we will need to convert the messages when they are ready to be sent.
        // 使用一个由所有代理支持的魔力版本，以减少我们在准备发送消息时需要转换消息的可能。
        byte maxUsableMagic = RecordBatch.CURRENT_MAGIC_VALUE;
        for (NodeApiVersions versions : this.nodeApiVersions.values()) {
            byte nodeMaxUsableMagic = ProduceRequest.requiredMagicForVersion(versions.latestUsableVersion(ApiKeys.PRODUCE));
            maxUsableMagic = (byte) Math.min(nodeMaxUsableMagic, maxUsableMagic);
        }
        return maxUsableMagic;
    }

    public synchronized byte maxUsableProduceMagic() {
        return maxUsableProduceMagic;
    }

}
