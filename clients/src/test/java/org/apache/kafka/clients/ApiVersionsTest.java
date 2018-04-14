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
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class ApiVersionsTest {

    @Test
    public void testMaxUsableProduceMagic() {
        System.out.println("5" + 2);
         String TRANSACTIONAL_ID_DOC = "The TransactionalId to use for transactional delivery. This enables reliability semantics which span multiple producer sessions since it allows the client to guarantee that transactions using the same TransactionalId have been completed prior to starting any new transactions. If no TransactionalId is provided, then the producer is limited to idempotent delivery. " +
                "Note that enable.idempotence must be enabled if a TransactionalId is configured. " +
                "The default is <code>null</code>, which means transactions cannot be used. " +
                "Note that transactions requires a cluster of at least three brokers by default what is the recommended setting for production; for development you can change this, by adjusting broker setting `transaction.state.log.replication.factor`.";

        ApiVersions apiVersions = new ApiVersions();
        assertEquals(RecordBatch.CURRENT_MAGIC_VALUE, apiVersions.maxUsableProduceMagic());

        apiVersions.update("0", NodeApiVersions.create());
        assertEquals(RecordBatch.CURRENT_MAGIC_VALUE, apiVersions.maxUsableProduceMagic());

        apiVersions.update("1", NodeApiVersions.create(Collections.singleton(
                new ApiVersionsResponse.ApiVersion(ApiKeys.PRODUCE.id, (short) 0, (short) 2))));
        assertEquals(RecordBatch.MAGIC_VALUE_V1, apiVersions.maxUsableProduceMagic());

        apiVersions.remove("1");
        assertEquals(RecordBatch.CURRENT_MAGIC_VALUE, apiVersions.maxUsableProduceMagic());
    }

}
