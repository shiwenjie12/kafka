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

package org.apache.kafka.common.record;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

/**
 * 用于缓存字节缓冲区的简单非线程安全接口。
 * 这适用于简单的情况，例如确保给定的KafkaConsumer在迭代获取的记录时重用相同的解压缩缓冲区。
 * 对于小的记录批次，分配潜在的大缓冲区（LZ4的64 KB）将支配在批处理中的记录的解压缩和迭代的成本。
 */
public abstract class BufferSupplier implements AutoCloseable {

    public static final BufferSupplier NO_CACHING = new BufferSupplier() {// 内部未实现快速缓存
        @Override
        public ByteBuffer get(int capacity) {
            return ByteBuffer.allocate(capacity);
        }

        @Override
        public void release(ByteBuffer buffer) {}

        @Override
        public void close() {}
    };

    public static BufferSupplier create() {
        return new DefaultSupplier();
    }

    /**
     * Supply a buffer with the required capacity. This may return a cached buffer or allocate a new instance.
     */
    public abstract ByteBuffer get(int capacity);

    /**
     * Return the provided buffer to be reused by a subsequent call to `get`.
     */
    public abstract void release(ByteBuffer buffer);

    /**
     * Release all resources associated with this supplier.
     */
    public abstract void close();

    private static class DefaultSupplier extends BufferSupplier {
        // We currently use a single block size, so optimise for that case
        private final Map<Integer, Deque<ByteBuffer>> bufferMap = new HashMap<>(1);

        @Override
        public ByteBuffer get(int size) {
            Deque<ByteBuffer> bufferQueue = bufferMap.get(size);
            if (bufferQueue == null || bufferQueue.isEmpty())
                return ByteBuffer.allocate(size);
            else
                return bufferQueue.pollFirst();
        }

        @Override
        public void release(ByteBuffer buffer) {
            buffer.clear();
            Deque<ByteBuffer> bufferQueue = bufferMap.get(buffer.capacity());
            if (bufferQueue == null) {
                // We currently keep a single buffer in flight, so optimise for that case
                bufferQueue = new ArrayDeque<>(1);
                bufferMap.put(buffer.capacity(), bufferQueue);
            }
            bufferQueue.addLast(buffer);
        }

        @Override
        public void close() {
            bufferMap.clear();
        }
    }
}
