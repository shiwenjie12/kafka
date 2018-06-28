/**
 *
 *
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.metrics

import kafka.utils.{VerifiableProperties, CoreUtils}

/**
  * kafka的监控配置
  * @param props
  */
class KafkaMetricsConfig(props: VerifiableProperties) {

  /**
    * 报告类型的逗号分隔列表。这些类应该在classpath，将在运行时实例化。
   */
  val reporters = CoreUtils.parseCsvList(props.getString("kafka.metrics.reporters", ""))

  /**
    * 监控每次拉取的时间间隔（秒）
   */
  val pollingIntervalSecs = props.getInt("kafka.metrics.polling.interval.secs", 10)
}
