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
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ipc.RpcSSLEngineAbstr;
import org.apache.hadoop.security.UserGroupInformation;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Paths;

public class HopsSSLSocketFactory extends SocketFactory implements Configurable {

    //public static final String KEY_STORE_FILEPATH_KEY = "client.rpc.ssl.keystore.filepath";
    private static final String KEY_STORE_FILEPATH_DEFAULT = "client.keystore.jks";
    //public static final String KEY_STORE_PASSWORD_KEY = "client.rpc.ssl.keystore.password";
    private static final String KEY_STORE_PASSWORD_DEFAULT = "";
    //public static final String KEY_PASSWORD_KEY = "client.rpc.ssl.keypassword";
    private static final String KEY_PASSWORD_DEFAULT = "";
    //public static final String TRUST_STORE_FILEPATH_KEY = "client.rpc.ssl.truststore.filepath";
    private static final String TRUST_STORE_FILEPATH_DEFAULT = "client.truststore.jks";
    //public static final String TRUST_STORE_PASSWORD_KEY = "client.rpc.ssl.truststore.password";
    private static final String TRUST_STORE_PASSWORD_DEFAULT = "";

    private final Log LOG = LogFactory.getLog(HopsSSLSocketFactory.class);

    private enum PropType {
        FILEPATH,
        LITERAL
    }

    public enum CryptoKeys {

        KEY_STORE_FILEPATH_KEY("client.rpc.ssl.keystore.filepath", KEY_STORE_FILEPATH_DEFAULT, PropType.FILEPATH),
        KEY_STORE_PASSWORD_KEY("client.rpc.ssl.keystore.password", KEY_STORE_PASSWORD_DEFAULT, PropType.LITERAL),
        KEY_PASSWORD_KEY("client.rpc.ssl.keypassword", KEY_PASSWORD_DEFAULT, PropType.LITERAL),
        TRUST_STORE_FILEPATH_KEY("client.rpc.ssl.truststore.filepath", TRUST_STORE_FILEPATH_DEFAULT, PropType.FILEPATH),
        TRUST_STORE_PASSWORD_KEY("client.rpc.ssl.truststore.password", TRUST_STORE_PASSWORD_DEFAULT, PropType.LITERAL);

        private final String value;
        private final String defaultValue;
        private final PropType type;

        CryptoKeys(String value, String defaultValue, PropType type) {
            this.value = value;
            this.defaultValue = defaultValue;
            this.type = type;
        }

        public String getValue() {
            return this.value;
        }

        public String getDefaultValue() {
            return this.defaultValue;
        }

        public PropType getType() {
            return type;
        }
    }

    private Configuration conf;
    private String keyStoreFilePath;
    // Hopsworks project specific username pattern - projectName__username
    private final String userPattern = "\\w*__\\w*";

    public HopsSSLSocketFactory() {
    }

    @Override
    public void setConf(Configuration conf) {
        try {
            String username = UserGroupInformation.getCurrentUser().getUserName();
            LOG.error("Current user's username is: " + username);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Current user's username is: " + username);
            }
            // First we check if the crypto material has been set in the configuration
            if (!isCryptoMaterialSet(conf, username)) {

                if (username.matches(userPattern)) {
                    // It's a normal user
                    // First check for the file in classpath
                    File fd = new File(username + "__kstore.jks");
                    if (fd.exists()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Crypto material exist in classpath");
                        }
                        configureTlsClient("", username, conf);
                    } else {
                        // Otherwise they should be in the materialized directory
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Crypto material exist in /tmp/");
                        }
                        configureTlsClient("/tmp/", username, conf);
                    }
                } else {
                    // It's the superuser
                    // Set the paths to the host crypto material
                    // For the moment is /tmp/glassfish__
                    configureTlsClient("/tmp/", username, conf);
                }
            }
        } catch (IOException ex) {
            LOG.error(ex, ex);
        }

        this.conf = conf;
        // *ClientCache* caches client instances based on their socket factory.
        // In order to distinguish two client with the same socket factory but
        // with different certificates, the hashCode is computed by the
        // keystore filepath as well
        this.keyStoreFilePath = conf.get(CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue(),
                KEY_STORE_FILEPATH_DEFAULT);
        LOG.error("<Kavouri> keystore used: " + keyStoreFilePath);
    }

    // TODO Use this also from Hopsworks
    public static void configureTlsClient(String filePrefix, String username, Configuration conf) {
        String pref = Paths.get(filePrefix, username).toString();
        conf.set(CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue(), pref + "__kstore.jks");
        conf.set(CryptoKeys.KEY_STORE_PASSWORD_KEY.getValue(), "adminpw");
        conf.set(CryptoKeys.KEY_PASSWORD_KEY.getValue(), "adminpw");
        conf.set(CryptoKeys.TRUST_STORE_FILEPATH_KEY.getValue(), pref + "__tstore.jks");
        conf.set(CryptoKeys.TRUST_STORE_PASSWORD_KEY.getValue(), "adminpw");
        conf.set(CommonConfigurationKeys.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY,
                "org.apache.hadoop.net.HopsSSLSocketFactory");
    }

    private boolean isCryptoMaterialSet(Configuration conf, String username) {
        for (CryptoKeys key : CryptoKeys.values()) {
            String propValue = conf.get(key.getValue(), key.getDefaultValue());
            if (key.getDefaultValue().equals(propValue)
                    || !checkUsernameInProperty(username, propValue, key.getType())) {
                return false;
            }
        }

        return true;
    }

    /**
     * Checks if the username is part of the property value. For example,
     * projectName__userName should be part of value /tmp/projectName__userName__kstore.jks
     * @param username
     * @param propValue
     * @return
     */
    private boolean checkUsernameInProperty(String username, String propValue, PropType propType) {
        // TODO Don't deal with this for the moment
        /*if (propType == PropType.FILEPATH) {
            return propValue.contains(username);
        }*/

        return true;
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
