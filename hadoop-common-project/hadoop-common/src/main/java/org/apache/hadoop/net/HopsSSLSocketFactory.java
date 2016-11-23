package org.apache.hadoop.net;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ipc.RpcSSLEngineAbstr;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.SecureRandom;

/**
 * Created by antonis on 11/21/16.
 */
public class HopsSSLSocketFactory extends SocketFactory {
    private final Log LOG = LogFactory.getLog(HopsSSLSocketFactory.class);
    private SSLContext sslCtx;

    public HopsSSLSocketFactory() {

    }

    public Socket createSocket() throws IOException, UnknownHostException {
        LOG.debug("Creating SSL client socket");
        try {
            this.sslCtx = SSLContext.getInstance("TLSv1.2");
            String keyStoreFilePath = "/home/antonis/SICS/keyStore.jks";
            String keyStorePasswd = "123456";
            String keyPasswd = "123456";
            this.sslCtx.init(RpcSSLEngineAbstr.createKeyManager(keyStoreFilePath, keyStorePasswd, keyPasswd),
                    RpcSSLEngineAbstr.createTrustManager(keyStoreFilePath, keyStorePasswd), new SecureRandom());
            SSLSocketFactory socketFactory = sslCtx.getSocketFactory();
            Socket socket = socketFactory.createSocket();
            return socket;
        } catch (Exception ex) {
            LOG.error(ex, ex);
            throw new IOException("Error while initializing cryptographic material");
        }
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException, UnknownHostException {
        Socket socket = createSocket();
        socket.connect(new InetSocketAddress(host, port));
        return socket;
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress localAddress,
            int localPort) throws IOException, UnknownHostException {
        Socket socket = createSocket();
        socket.bind(new InetSocketAddress(localAddress, localPort));
        socket.connect(new InetSocketAddress(host, port));
        return socket;
    }

    @Override
    public Socket createSocket(InetAddress inetAddress, int port) throws IOException {
        Socket socket = createSocket();
        socket.connect(new InetSocketAddress(inetAddress, port));
        return socket;
    }

    @Override
    public Socket createSocket(InetAddress inetAddress, int port, InetAddress localAddress, int localPort)
            throws IOException {
        Socket socket = createSocket();
        socket.bind(new InetSocketAddress(localAddress, localPort));
        socket.connect(new InetSocketAddress(inetAddress, port));
        return socket;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        return obj.getClass().equals(this.getClass());
    }

    @Override
    public int hashCode() {
        return this.getClass().hashCode();
    }
}
