/**
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
package kafka.admin

/**
  * 控制如何执行机架感知副本分配的模式
  */
object RackAwareMode {

  /**
    * 忽略复制分配中的所有机架信息。这是命令行中使用的可选模式。.
    */
  case object Disabled extends RackAwareMode

  /**
    * 假设每个经纪人都有架子，或者broker没有架子。如果只有部分代理有机架，在副本分配中失败很快。
    * 这是命令行工具中的默认模式（Popic命令和RealDebug分区命令）。
    */
  case object Enforced extends RackAwareMode

  /**
    * 如果每个代理都有机架，则使用机架信息。否则，回落到禁用模式。这用于自动主题创建。
    */
  case object Safe extends RackAwareMode
}

sealed trait RackAwareMode
