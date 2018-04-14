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

/**
 * STAT是指从更新到传感器的流计算出来的数量，如平均值、最大值等。
 */
public interface Stat {

    /**
     * 记录给定的值
     * @param config The configuration to use for this metric
     * @param value The value to record
     * @param timeMs The POSIX time in milliseconds this value occurred
     */
    public void record(MetricConfig config, double value, long timeMs);

}
