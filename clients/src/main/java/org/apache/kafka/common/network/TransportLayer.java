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

/*
 * Transport layer for underlying communication.
 * At very basic level it is wrapper around SocketChannel and can be used as substitute for SocketChannel
 * and other network Channel implementations.
 * As NetworkClient replaces BlockingChannel and other implementations we will be using KafkaChannel as
 * a network I/O channel.
 * 底层通信的传输层。
 * 在最基本的水平是包裹SocketChannel，可用于代替SocketChannel和其他网络信道实现。
 * 作为networkclient取代blockingchannel等实现，我们将使用kafkachannel作为网络I/O通道。
 */
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.GatheringByteChannel;

import java.security.Principal;

import org.apache.kafka.common.errors.AuthenticationException;

public interface TransportLayer extends ScatteringByteChannel, GatheringByteChannel {

    /**
     * 如果通道确认握手并且已经验证，则返回true
     */
    boolean ready();

    /**
     * 完成连接套接字通道
     */
    boolean finishConnect() throws IOException;

    /**
     * 断开连接
     */
    void disconnect();

    /**
     * Tells whether or not this channel's network socket is connected.
     */
    boolean isConnected();

    /**
     * 返回下层的socketChannel
     */
    SocketChannel socketChannel();

    /**
     * 返回下层的selection key
     */
    SelectionKey selectionKey();

    /**
     * This a no-op for the non-secure PLAINTEXT implementation. For SSL, this performs
     * SSL handshake. The SSL handshake includes client authentication if configured using
     * {@link org.apache.kafka.common.config.SslConfigs#SSL_CLIENT_AUTH_CONFIG}.
     * @throws AuthenticationException if handshake fails due to an {@link javax.net.ssl.SSLException}.
     * @throws IOException if read or write fails with an I/O error.
    */
    void handshake() throws AuthenticationException, IOException;

    /**
     * 如果有挂起的写入，则返回true
     */
    boolean hasPendingWrites();

    /**
     * Returns `SSLSession.getPeerPrincipal()` if this is a SslTransportLayer and there is an authenticated peer,
     * `KafkaPrincipal.ANONYMOUS` is returned otherwise.
     */
    Principal peerPrincipal() throws IOException;

    void addInterestOps(int ops);

    void removeInterestOps(int ops);

    boolean isMute();

    /**
     * @return 如果通道有在任何中间缓冲区中读取的字节，则为true。
     */
    boolean hasBytesBuffered();

    /**
     * Transfers bytes from `fileChannel` to this `TransportLayer`.
     * 将字节从` FileChannel `这` `传输层。
     * This method will delegate to {@link FileChannel#transferTo(long, long, java.nio.channels.WritableByteChannel)},
     * but it will unwrap the destination channel, if possible, in order to benefit from zero copy. This is required
     * because the fast path of `transferTo` is only executed if the destination buffer inherits from an internal JDK
     * class.
     *
     * @param fileChannel The source channel
     * @param position The position within the file at which the transfer is to begin; must be non-negative
     * @param count The maximum number of bytes to be transferred; must be non-negative
     * @return The number of bytes, possibly zero, that were actually transferred
     * @see FileChannel#transferTo(long, long, java.nio.channels.WritableByteChannel)
     */
    long transferFrom(FileChannel fileChannel, long position, long count) throws IOException;
}
