package org.apache.hadoop.net;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RpcSSLEngineAbstr;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * Created by antonis on 11/21/16.
 */
public class HopsSSLSocketFactory extends SocketFactory implements Configurable {

    // TODO: Choose sensible default values, for the moment it's fine
    public static final String KEY_STORE_FILEPATH_KEY = "client.rpc.ssl.keystore.filepath";
    public static final String KEY_STORE_FILEPATH_DEFAULT = "/home/antonis/SICS/key_material/client.keystore.jks";
    public static final String KEY_STORE_PASSWORD_KEY = "client.rpc.ssl.keystore.password";
    public static final String KEY_STORE_PASSWORD_DEFAULT = "123456";
    public static final String KEY_PASSWORD_KEY = "client.rpc.ssl.keypassword";
    public static final String KEY_PASSWORD_DEFAULT = "123456";
    public static final String TRUST_STORE_FILEPATH_KEY = "client.rpc.ssl.truststore.filepath";
    public static final String TRUST_STORE_FILEPATH_DEFAULT = "/home/antonis/SICS/key_material/client.truststore.jks";
    public static final String TRUST_STORE_PASSWORD_KEY = "client.rpc.ssl.truststore.password";
    public static final String TRUST_STORE_PASSWORD_DEFAULT = "123456";

    private final Log LOG = LogFactory.getLog(HopsSSLSocketFactory.class);

    private Configuration conf;
    private String keyStoreFilePath;

    public HopsSSLSocketFactory() {

    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        this.keyStoreFilePath = conf.get(KEY_STORE_FILEPATH_KEY,
                KEY_STORE_FILEPATH_DEFAULT);
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    public Socket createSocket() throws IOException, UnknownHostException {
        LOG.debug("Creating SSL client socket");
        SSLContext sslCtx = RpcSSLEngineAbstr.initializeSSLContext(conf);
        SSLSocketFactory socketFactory = sslCtx.getSocketFactory();
        return socketFactory.createSocket();
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

    public String getKeyStoreFilePath() {
        return keyStoreFilePath;
    }

    @Override
    public boolean equals(Object obj) {
        /*if (this == obj)
            return true;
        if (obj == null)
            return false;
        return obj.getClass().equals(this.getClass());*/

        if (obj instanceof HopsSSLSocketFactory) {

            return this == obj || ((HopsSSLSocketFactory) obj).getKeyStoreFilePath().equals(this.getKeyStoreFilePath());
        }

        return false;
    }

    @Override
    public int hashCode() {
        int result = 3;
        result = 37 * result + this.getClass().hashCode();
        result = 37 * result + this.keyStoreFilePath.hashCode();

        return result;
    }
}
