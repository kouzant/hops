package org.apache.hadoop.ipc;

import com.sun.jersey.json.impl.FilteringInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import javax.net.ssl.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.security.KeyStore;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by antonis on 11/21/16.
 */
public class RpcSSLEngineImpl implements RpcSSLEngine {

    private final Log LOG = LogFactory.getLog(RpcSSLEngineImpl.class);
    private final SocketChannel socketChannel;
    private final SSLEngine sslEngine;
    private final ExecutorService exec = Executors.newSingleThreadExecutor();

    private final boolean SKATA = false;

    /**
     *
     *          serverApp   clientApp
     *          Buffer      Buffer
     *
     *              |           ^
     *              |     |     |
     *              v     |     |
     *         +----+-----|-----+----+
     *         |          |          |
     *         |       SSL|Engine    |
     * wrap()  |          |          |  unwrap()
     *         | OUTBOUND | INBOUND  |
     *         |          |          |
     *         +----+-----|-----+----+
     *              |     |     ^
     *              |     |     |
     *              v           |
     *
     *          serverNet   clientNet
     *          Buffer      Buffer
     */
    private ByteBuffer serverAppBuffer;
    private ByteBuffer clientAppBuffer;
    private ByteBuffer serverNetBuffer;
    private ByteBuffer clientNetBuffer;

    public RpcSSLEngineImpl(SocketChannel socketChannel, SSLEngine sslEngine) {
        this.socketChannel = socketChannel;
        this.sslEngine = sslEngine;
        serverAppBuffer = ByteBuffer.allocate(sslEngine.getSession().getApplicationBufferSize());
        clientAppBuffer = ByteBuffer.allocate(sslEngine.getSession().getApplicationBufferSize());
        serverNetBuffer = ByteBuffer.allocate(sslEngine.getSession().getPacketBufferSize());
        clientNetBuffer = ByteBuffer.allocate(sslEngine.getSession().getPacketBufferSize());
    }

    @Override
    public boolean doHandshake() throws IOException {
        if (SKATA) {
            return true;
        }
        LOG.debug("Starting TLS handshake with peer");

        SSLEngineResult result;
        SSLEngineResult.HandshakeStatus handshakeStatus;

        ByteBuffer serverAppBuffer = ByteBuffer.allocate(sslEngine.getSession().getApplicationBufferSize());
        ByteBuffer clientAppBuffer = ByteBuffer.allocate(sslEngine.getSession().getApplicationBufferSize());
        serverNetBuffer.clear();
        clientNetBuffer.clear();

        handshakeStatus = sslEngine.getHandshakeStatus();
        while (handshakeStatus != SSLEngineResult.HandshakeStatus.FINISHED
                && handshakeStatus != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
            switch (handshakeStatus) {
                case NEED_UNWRAP:
                    if (socketChannel.read(clientNetBuffer) < 0) {
                        if (sslEngine.isInboundDone() && sslEngine.isOutboundDone()) {
                            return false;
                        }
                        try {
                            sslEngine.closeInbound();
                        } catch (SSLException ex) {
                            LOG.error(ex, ex);
                        }
                        sslEngine.closeOutbound();
                        handshakeStatus = sslEngine.getHandshakeStatus();
                        break;
                    }
                    clientNetBuffer.flip();
                    try {
                        result = unwrap(clientNetBuffer, clientAppBuffer);
                        clientNetBuffer.compact();
                        handshakeStatus = result.getHandshakeStatus();
                    } catch (SSLException ex) {
                        LOG.error(ex, ex);
                        sslEngine.closeOutbound();
                        handshakeStatus = sslEngine.getHandshakeStatus();
                        break;
                    }
                    switch (result.getStatus()) {
                        case OK:
                            break;
                        case BUFFER_OVERFLOW:
                            // clientAppBuffer is not large enough
                            clientAppBuffer = enlargeApplicationBuffer(clientAppBuffer);
                            break;
                        case BUFFER_UNDERFLOW:
                            // Not enough input data to unwrap or the input buffer is too small
                            clientNetBuffer = handleBufferUnderflow(clientNetBuffer);
                            break;
                        case CLOSED:
                            if (sslEngine.isOutboundDone()) {
                                return false;
                            } else {
                                sslEngine.closeOutbound();
                                handshakeStatus = sslEngine.getHandshakeStatus();
                                break;
                            }
                        default:
                            throw new IllegalStateException("Invalid SSL status: " + result.getStatus());
                    }
                    break;
                case NEED_WRAP:
                    serverNetBuffer.clear();
                    try {
                        result = wrap(serverAppBuffer, serverNetBuffer);
                        handshakeStatus = result.getHandshakeStatus();
                    } catch (SSLException ex) {
                        LOG.error(ex, ex);
                        sslEngine.closeOutbound();
                        handshakeStatus = sslEngine.getHandshakeStatus();
                        break;
                    }
                    switch (result.getStatus()) {
                        case OK:
                            serverNetBuffer.flip();
                            while (serverNetBuffer.hasRemaining()) {
                                socketChannel.write(serverNetBuffer);
                            }
                            break;
                        case BUFFER_OVERFLOW:
                            serverNetBuffer = enlargePacketBuffer(serverNetBuffer);
                            break;
                        case BUFFER_UNDERFLOW:
                            throw new SSLException("Buffer overflow occured after a wrap.");
                        case CLOSED:
                            try {
                                serverNetBuffer.flip();
                                while (serverNetBuffer.hasRemaining()) {
                                    socketChannel.write(serverNetBuffer);
                                }
                                clientNetBuffer.clear();
                            } catch (Exception ex) {
                                LOG.error(ex, ex);
                                handshakeStatus = sslEngine.getHandshakeStatus();
                            }
                            break;
                        default:
                            throw new IllegalStateException("Invalid SSL status: " + result.getStatus());
                    }
                    break;
                case NEED_TASK:
                    Runnable task;
                    while ((task = sslEngine.getDelegatedTask()) != null) {
                        exec.execute(task);
                    }
                    handshakeStatus = sslEngine.getHandshakeStatus();
                    break;
                case FINISHED:
                    break;
                case NOT_HANDSHAKING:
                    break;
                default:
                    throw new IllegalStateException("Invalid SSL status: " + handshakeStatus);
            }
        }

        LOG.debug("SSL handshake finished successfully");
        return true;
    }

    @Override
    public SSLEngineResult wrap(ByteBuffer src, ByteBuffer dst) throws SSLException {
        return sslEngine.wrap(src, dst);
    }

    @Override
    public SSLEngineResult unwrap(ByteBuffer src, ByteBuffer dst) throws SSLException {
        return sslEngine.unwrap(src, dst);
    }

    @Override
    public int write(WritableByteChannel channel, ByteBuffer buffer)
        throws IOException {
        serverAppBuffer.clear();
        serverAppBuffer.put(buffer);
        serverAppBuffer.flip();

        while (serverAppBuffer.hasRemaining()) {
            serverNetBuffer.clear();
            SSLEngineResult result = wrap(serverAppBuffer, serverNetBuffer);
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
    public int decryptData(ReadableByteChannel channel, ByteBuffer buffer)
        throws IOException {
        clientNetBuffer.clear();
        int bytesRead = channel.read(clientNetBuffer);
        if (bytesRead > 0) {
            clientNetBuffer.flip();
            while (clientNetBuffer.hasRemaining()) {
                clientAppBuffer.clear();
                SSLEngineResult result = unwrap(clientNetBuffer, clientAppBuffer);
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

    @Override
    public int read(ReadableByteChannel channel, ByteBuffer buffer)
        throws IOException {

        clientNetBuffer.clear();
        int bytesRead = channel.read(clientNetBuffer);
        if (bytesRead > 0) {
            clientNetBuffer.flip();
            while (clientNetBuffer.hasRemaining()) {
                clientAppBuffer.clear();
                buffer.clear();
                SSLEngineResult result = unwrap(clientNetBuffer, clientAppBuffer);
                switch (result.getStatus()) {
                    case OK:
                        clientAppBuffer.flip();
                        LOG.debug("Server decrypted incoming message");
                        buffer.put(clientAppBuffer.array(), 0, buffer.capacity());

                        return bytesRead;
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
        } else {
            /*sslEngine.closeInbound();
            sslEngine.closeOutbound();
            doHandshake();*/
            return -1;
        }

        return -1;
    }

    @Override
    public void closeOutbound() {
        sslEngine.closeOutbound();
    }

    private ByteBuffer enlargeApplicationBuffer(ByteBuffer buffer) {
        return enlargeBuffer(buffer, sslEngine.getSession().getApplicationBufferSize());
    }

    private ByteBuffer enlargePacketBuffer(ByteBuffer buffer) {
        return enlargeBuffer(buffer, sslEngine.getSession().getPacketBufferSize());
    }

    private ByteBuffer enlargeBuffer(ByteBuffer buffer, int sessionProposedCapacity) {
        if (sessionProposedCapacity > buffer.capacity()) {
            return ByteBuffer.allocate(sessionProposedCapacity);
        } else {
            return ByteBuffer.allocate(buffer.capacity() * 2);
        }
    }

    private ByteBuffer handleBufferUnderflow(ByteBuffer buffer) {
        // If there is no size issue, return the same buffer and let the
        // peer read more data
        if (sslEngine.getSession().getPacketBufferSize() < buffer.limit()) {
            return buffer;
        } else {
            ByteBuffer newBuffer = enlargePacketBuffer(buffer);
            buffer.flip();
            newBuffer.put(buffer);
            return newBuffer;
        }
    }

    // Helper method to initialize key managers needed by SSLContext
    public static KeyManager[] createKeyManager(String filePath, String keyStorePasswd, String keyPasswd) throws Exception {
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(new FileInputStream(filePath), keyStorePasswd.toCharArray());

        KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
        kmf.init(keyStore, keyPasswd.toCharArray());

        return kmf.getKeyManagers();
    }

    // Helper method to initialize trust manager needed by SSLContext
    public static TrustManager[] createTrustManager(String filePath, String keyStorePasswd) throws Exception {
        KeyStore trustStore = KeyStore.getInstance("JKS");
        trustStore.load(new FileInputStream(filePath), keyStorePasswd.toCharArray());

        TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
        tmf.init(trustStore);

        return tmf.getTrustManagers();
    }
}
