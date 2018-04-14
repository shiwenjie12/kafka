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
package org.apache.kafka.common.metrics;

import java.util.List;

import org.apache.kafka.common.Configurable;

/**
 * 一个插件接口，允许新的度量值创建时可以监听，这样就可以报告。
 * <p>
 * Implement {@link org.apache.kafka.common.ClusterResourceListener} to receive cluster metadata once it's available.
 * Please see the class documentation for ClusterResourceListener for more information.
 */
public interface MetricsReporter extends Configurable {

    /**
     * 当第一次注册时，它首先注册所有现有的指标。
     * @param metrics All currently existing metrics
     */
    public void init(List<KafkaMetric> metrics);

    /**
     * 每当更新或添加度量时调用这个函数。
     * @param metric
     */
    public void metricChange(KafkaMetric metric);

    /**
     * 每当删除一个度量值时调用这个函数。
     * @param metric
     */
    public void metricRemoval(KafkaMetric metric);

    /**
     * Called when the metrics repository is closed.
     */
    public void close();

}
