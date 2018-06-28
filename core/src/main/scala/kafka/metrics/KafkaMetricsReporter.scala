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

import kafka.utils.{CoreUtils, VerifiableProperties}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable.ArrayBuffer


/**
 * Base trait for reporter MBeans. If a client wants to expose these JMX
 * operations on a custom reporter (that implements
 * [[kafka.metrics.KafkaMetricsReporter]]), the custom reporter needs to
 * additionally implement an MBean trait that extends this trait so that the
 * registered MBean is compliant with the standard MBean convention.
 */
trait KafkaMetricsReporterMBean {
  def startReporter(pollingPeriodInSeconds: Long)
  def stopReporter()

  /**
   *
   * @return The name with which the MBean will be registered.
   */
  def getMBeanName: String
}

/**
  * 实现{@link org.apache.kafka.common.ClusterResourceListener}一次可获得集群元数据。
  */
trait KafkaMetricsReporter {
  def init(props: VerifiableProperties)
}

object KafkaMetricsReporter {
  val ReporterStarted: AtomicBoolean = new AtomicBoolean(false)
  private var reporters: ArrayBuffer[KafkaMetricsReporter] = null

  /**
    * 根据配置属性注册JMX的报表
    * @param verifiableProps
    * @return
    */
  def startReporters (verifiableProps: VerifiableProperties): Seq[KafkaMetricsReporter] = {
    ReporterStarted synchronized {
      if (!ReporterStarted.get()) {
        reporters = ArrayBuffer[KafkaMetricsReporter]()
        val metricsConfig = new KafkaMetricsConfig(verifiableProps)
        if(metricsConfig.reporters.nonEmpty) {// 加载监控
          metricsConfig.reporters.foreach(reporterType => {
            val reporter = CoreUtils.createObject[KafkaMetricsReporter](reporterType)// 创建监控对象
            reporter.init(verifiableProps)// 监控初始化
            reporters += reporter
            reporter match {// 如果监控继承了KafkaMetricsReporterMBean，则注册JMX
              case bean: KafkaMetricsReporterMBean => CoreUtils.registerMBean(reporter, bean.getMBeanName)
              case _ =>
            }
          })
          ReporterStarted.set(true)
        }
      }
    }
    reporters
  }
}

