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

public class HopsSSLSocketFactory extends SocketFactory implements Configurable {

    public static final String KEY_STORE_FILEPATH_KEY = "client.rpc.ssl.keystore.filepath";
    public static final String KEY_STORE_FILEPATH_DEFAULT = "client.keystore.jks";
    public static final String KEY_STORE_PASSWORD_KEY = "client.rpc.ssl.keystore.password";
    public static final String KEY_STORE_PASSWORD_DEFAULT = "";
    public static final String KEY_PASSWORD_KEY = "client.rpc.ssl.keypassword";
    public static final String KEY_PASSWORD_DEFAULT = "";
    public static final String TRUST_STORE_FILEPATH_KEY = "client.rpc.ssl.truststore.filepath";
    public static final String TRUST_STORE_FILEPATH_DEFAULT = "client.truststore.jks";
    public static final String TRUST_STORE_PASSWORD_KEY = "client.rpc.ssl.truststore.password";
    public static final String TRUST_STORE_PASSWORD_DEFAULT = "";

    private final Log LOG = LogFactory.getLog(HopsSSLSocketFactory.class);

    private Configuration conf;
    private String keyStoreFilePath;

    public HopsSSLSocketFactory() {
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        // *ClientCache* caches client instances based on their socket factory.
        // In order to distinguish two client with the same socket factory but
        // with different certificates, the hashCode is computed by the
        // keystore filepath as well
        this.keyStoreFilePath = conf.get(KEY_STORE_FILEPATH_KEY,
                KEY_STORE_FILEPATH_DEFAULT);
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    public Socket createSocket() throws IOException, UnknownHostException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating SSL client socket");
        }
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
        if (obj instanceof HopsSSLSocketFactory) {

            return this == obj || ((HopsSSLSocketFactory) obj)
                    .getKeyStoreFilePath().equals(this.getKeyStoreFilePath());
        }

        return false;
    }

    @Override
    public int hashCode() {
        int result = 3;
        result = 37 * result + this.getClass().hashCode();
        // See comment at setConf
        result = 37 * result + this.keyStoreFilePath.hashCode();

        return result;
    }
}
