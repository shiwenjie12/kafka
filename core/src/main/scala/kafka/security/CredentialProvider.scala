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

package kafka.security

import java.util.{Collection, Properties}

import org.apache.kafka.common.security.authenticator.CredentialCache
import org.apache.kafka.common.security.scram.{ScramCredential, ScramCredentialUtils, ScramMechanism}
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef._
import org.apache.kafka.common.security.token.delegation.DelegationTokenCache

// 凭证的供应器
class CredentialProvider(scramMechanisms: Collection[String], val tokenCache: DelegationTokenCache) {

  val credentialCache = new CredentialCache
  ScramCredentialUtils.createCache(credentialCache, scramMechanisms)

  // 更新凭据
  def updateCredentials(username: String, config: Properties) {
    for (mechanism <- ScramMechanism.values()) { // 机制 SHA-256、SHA-512
      val cache = credentialCache.cache(mechanism.mechanismName, classOf[ScramCredential])
      if (cache != null) { // 获取机制缓存对象
        config.getProperty(mechanism.mechanismName) match {
          case null => cache.remove(username)
          case c => cache.put(username, ScramCredentialUtils.credentialFromString(c)) // 更新缓存中的username的属性
        }
      }
    }
  }
}

object CredentialProvider {
  def userCredentialConfigs: ConfigDef = {
    ScramMechanism.values.foldLeft(new ConfigDef) {
      (c, m) => c.define(m.mechanismName, Type.STRING, null, Importance.MEDIUM, s"User credentials for SCRAM mechanism ${m.mechanismName}")
    }
  }
}

