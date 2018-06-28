/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package kafka.server

import java.util.Properties

import kafka.metrics.KafkaMetricsReporter
import kafka.utils.{Exit, Logging, VerifiableProperties}

/**
  * kafka的启动类
  */
object KafkaServerStartable {
  def fromProps(serverProps: Properties) = {
    val reporters = KafkaMetricsReporter.startReporters(new VerifiableProperties(serverProps))// 返回监控对象
    new KafkaServerStartable(KafkaConfig.fromProps(serverProps, false), reporters)
  }
}

/**
  * 创建Kafka的启动类
  * @param staticServerConfig
  * @param reporters
  */
class KafkaServerStartable(val staticServerConfig: KafkaConfig, reporters: Seq[KafkaMetricsReporter]) extends Logging {
  private val server = new KafkaServer(staticServerConfig, kafkaMetricsReporters = reporters)

  def this(serverConfig: KafkaConfig) = this(serverConfig, Seq.empty)

  /**
    * 启动方法
    */
  def startup() {
    try server.startup()
    catch {
      case _: Throwable =>
        // KafkaServer.startup() calls shutdown() in case of exceptions, so we invoke `exit` to set the status code
        fatal("Exiting Kafka.")
        Exit.exit(1)
    }
  }

  /**
    * 关闭方法
    */
  def shutdown() {
    try server.shutdown()
    catch {
      case _: Throwable =>
        fatal("Halting Kafka.")
        // Calling exit() can lead to deadlock as exit() can be called multiple times. Force exit.
        Exit.halt(1)
    }
  }

  /**
    * 允许从启动设置代理的状态。这是需要一个定制的卡夫卡服务器启动想发出新规定，介绍了。
    */
  def setServerState(newState: Byte) {
    server.brokerState.newState(newState)
  }

  def awaitShutdown(): Unit = server.awaitShutdown()

}


