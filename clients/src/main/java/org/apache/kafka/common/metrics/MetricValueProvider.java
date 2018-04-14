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
 * 用于提供链接的{@link Measurable}或{@link Gauge} 的超级接口度量值。
 * <p>
 * In the future for Java8 and above, {@link Gauge#value(MetricConfig, long)} will be
 * moved to this interface with a default implementation in {@link Measurable} that returns
 * {@link Measurable#measure(MetricConfig, long)}.
 * </p>
 */
public interface MetricValueProvider<T> { }
