package org.apache.hadoop.ipc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Created by antonis on 11/23/16.
 */
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
                    LOG.debug("Encrypted and sent back response");
                    return bytesWritten;
                case BUFFER_OVERFLOW:
                    serverNetBuffer = enlargePacketBuffer(serverNetBuffer);
                    break;
                case BUFFER_UNDERFLOW:
                    throw new SSLException("Buffer underflow should not happen after wrap");
                case CLOSED:
                    LOG.debug("Client closed the connection while trying to write");
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
                        LOG.debug("Decrypted data");
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
                        LOG.debug("Client closed the connection while trying to read");
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
}
