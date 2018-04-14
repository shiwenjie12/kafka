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
package org.apache.kafka.common.network;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.ScatteringByteChannel;

/**
 * 此接口模型从一个通道到一个由整数id标识的源的数据正在进行的读取。
 */
public interface Receive extends Closeable {

    /**
     * 我们正在接收数据的源的数字id。
     */
    String source();

    /**
     * Are we done receiving data?
     */
    boolean complete();

    /**
     * 从给定通道读取字节到这个接收中
     * @param channel The channel to read from
     * @return The number of bytes read
     * @throws IOException If the reading fails
     */
    long readFrom(ScatteringByteChannel channel) throws IOException;

    /**
     * Do we know yet how much memory we require to fully read this
     */
    boolean requiredMemoryAmountKnown();

    /**
     * Has the underlying memory required to complete reading been allocated yet?
     */
    boolean memoryAllocated();
}
