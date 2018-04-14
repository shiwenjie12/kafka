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

/**
 * 节点的连接状态
 *
 * DISCONNECTED: 连接尚未成功建立
 * CONNECTING: 连接正在进行中。
 * CHECKING_API_VERSIONS: 已建立连接，API版本检查正在进行中。此检查失败将导致连接关闭。
 * READY: 连接已准备好发送请求。
 * AUTHENTICATION_FAILED: 由于身份验证错误，连接失败。
 */
public enum ConnectionState {
    DISCONNECTED, CONNECTING, CHECKING_API_VERSIONS, READY, AUTHENTICATION_FAILED;

    public boolean isDisconnected() {
        return this == AUTHENTICATION_FAILED || this == DISCONNECTED;
    }
}
