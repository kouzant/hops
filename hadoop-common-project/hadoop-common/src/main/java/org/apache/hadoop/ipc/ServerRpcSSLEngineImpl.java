/*
 * Copyright 2016 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ipc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLPeerUnverifiedException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.security.cert.X509Certificate;

public class ServerRpcSSLEngineImpl extends RpcSSLEngineAbstr {
    private final Log LOG = LogFactory.getLog(ServerRpcSSLEngineImpl.class);

    public ServerRpcSSLEngineImpl(SocketChannel socketChannel, SSLEngine sslEngine) {
        super(socketChannel, sslEngine);
    }

    @Override
    public int write(WritableByteChannel channel, ByteBuffer buffer)
            throws IOException {
        serverAppBuffer.clear();
        serverAppBuffer.put(buffer);
        serverAppBuffer.flip();

        while (serverAppBuffer.hasRemaining()) {
            serverNetBuffer.clear();
            SSLEngineResult result = sslEngine.wrap(serverAppBuffer, serverNetBuffer);
            switch (result.getStatus()) {
                case OK:
                    serverNetBuffer.flip();
                    int bytesWritten = 0;
                    while (serverNetBuffer.hasRemaining()) {
                        bytesWritten += channel.write(serverNetBuffer);
                    }
                    return bytesWritten;
                case BUFFER_OVERFLOW:
                    serverNetBuffer = enlargePacketBuffer(serverNetBuffer);
                    break;
                case BUFFER_UNDERFLOW:
                    throw new SSLException("Buffer underflow should not happen after wrap");
                case CLOSED:
                    sslEngine.closeOutbound();
                    doHandshake();
                    return -1;
                default:
                    throw new IllegalStateException("Invalid SSL state: " + result.getStatus());
            }
        }
        return -1;
    }

    @Override
    public int read(ReadableByteChannel channel, ByteBuffer buffer)
            throws IOException {
        clientNetBuffer.clear();
        int bytesRead = channel.read(clientNetBuffer);
        if (bytesRead > 0) {
            clientNetBuffer.flip();
            while (clientNetBuffer.hasRemaining()) {
                clientAppBuffer.clear();
                SSLEngineResult result = sslEngine.unwrap(clientNetBuffer, clientAppBuffer);
                switch (result.getStatus()) {
                    case OK:
                        clientAppBuffer.flip();
                        while (clientAppBuffer.hasRemaining()) {
                            buffer.put(clientAppBuffer.get());
                        }
                        break;
                    case BUFFER_OVERFLOW:
                        clientAppBuffer = enlargeApplicationBuffer(clientAppBuffer);
                        break;
                    case BUFFER_UNDERFLOW:
                        clientNetBuffer = handleBufferUnderflow(clientNetBuffer);
                        break;
                    case CLOSED:
                        sslEngine.closeOutbound();
                        doHandshake();
                        return -1;
                    default:
                        throw new IllegalStateException("Invalid SSL status: " + result.getStatus());
                }
            }
        }
        return bytesRead;
    }

    public X509Certificate getClientCertificate() throws SSLPeerUnverifiedException {
        // The first certificate is always the peer's own certificate
        // https://docs.oracle.com/javase/8/docs/api/javax/net/ssl/SSLSession.html#getPeerCertificates--
        return (X509Certificate) sslEngine.getSession().getPeerCertificates()[0];
    }
}
