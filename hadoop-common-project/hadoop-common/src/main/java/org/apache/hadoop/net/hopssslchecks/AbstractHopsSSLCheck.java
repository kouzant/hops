/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.net.hopssslchecks;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.HopsSSLSocketFactory;
import org.apache.hadoop.security.ssl.CertificateLocalization;
import org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory;
import org.apache.hadoop.security.ssl.SSLFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

public abstract class AbstractHopsSSLCheck implements HopsSSLCheck, Comparable<HopsSSLCheck> {
  private final Integer priority;
  
  public AbstractHopsSSLCheck(Integer priority) {
    this.priority = priority;
  }
  
  public abstract HopsSSLCryptoMaterial check(String username, Set<String> proxySuperUsers,
      Configuration configuration, CertificateLocalization certificateLocalization)
      throws IOException, SSLMaterialAlreadyConfiguredException;
  
  @Override
  public Integer getPriority() {
    return priority;
  }
  
  @Override
  public int compareTo(HopsSSLCheck hopsSSLCheck) {
    return priority.compareTo(hopsSSLCheck.getPriority());
  }
  
  protected void isConfigurationNeededForNormalUser(String username, Configuration configuration)
    throws SSLMaterialAlreadyConfiguredException {
    // If already configured
    if (isCryptoMaterialSet(configuration, username)
        && !configuration.getBoolean(HopsSSLSocketFactory.FORCE_CONFIGURE,
        HopsSSLSocketFactory.DEFAULT_FORCE_CONFIGURE)) {
      throw new SSLMaterialAlreadyConfiguredException("Crypto material for user <" + username + "> has already been" +
          " configured");
    }
  }
  
  protected void isConfigurationNeededForSuperUser(String username, String hostname, Configuration configuration)
    throws SSLMaterialAlreadyConfiguredException {
    if (isCryptoMaterialSet(configuration, username)
        || isHostnameInCryptoMaterial(hostname, configuration)
        && !configuration.getBoolean(HopsSSLSocketFactory.FORCE_CONFIGURE,
        HopsSSLSocketFactory.DEFAULT_FORCE_CONFIGURE)) {
      throw new SSLMaterialAlreadyConfiguredException("Crypto material for user <" + username + "> has already been" +
          " configured");
    }
  }
  
  protected HopsSSLCryptoMaterial readSuperuserMaterialFromFile(Configuration configuration) throws IOException {
    Configuration sslConf = new Configuration(false);
    String sslConfResource = configuration.get(SSLFactory.SSL_SERVER_CONF_KEY, "ssl-server.xml");
    
    File sslConfFile = new File(sslConfResource);
    if (!sslConfFile.exists()) {
      String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
      if (hadoopConfDir == null) {
        hadoopConfDir = System.getProperty("HADOOP_CONF_DIR");
      }
      if (hadoopConfDir == null) {
        throw new IOException("JVM property -DHADOOP_CONF_DIR or " +
            "environment variable is not exported and " + sslConfResource +
            " is not in classpath");
      }
      Path sslConfPath = Paths.get(hadoopConfDir, sslConfResource);
      sslConfFile = sslConfPath.toFile();
    }
    
    if (!sslConfFile.exists()) {
      throw new IOException("Could not locate ssl-server.xml. Export " +
          "JVM property -DHADOOP_CONF_DIR or environment variable or add " +
          sslConfResource + " to classpath.");
    }
    FileInputStream sslConfIn = null;
    
    try {
      try {
        sslConfIn = new FileInputStream(sslConfFile);
        sslConf.addResource(sslConfIn);
      } catch (IOException ex) {
        sslConf.addResource(sslConfFile.getAbsolutePath());
      }
      
      String keystoreLocation = sslConf.get(
          FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
              FileBasedKeyStoresFactory.SSL_KEYSTORE_LOCATION_TPL_KEY));
      String keystorePassword = sslConf.get(
          FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
              FileBasedKeyStoresFactory.SSL_KEYSTORE_PASSWORD_TPL_KEY));
      String truststoreLocation = sslConf.get(
          FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
              FileBasedKeyStoresFactory.SSL_TRUSTSTORE_LOCATION_TPL_KEY));
      String truststorePassword = sslConf.get(
          FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
              FileBasedKeyStoresFactory.SSL_TRUSTSTORE_PASSWORD_TPL_KEY));
      
      return new HopsSSLCryptoMaterial(keystoreLocation, keystorePassword, truststoreLocation, truststorePassword);
    } finally {
      if (null != sslConfIn) {
        try {
          sslConfIn.close();
        } catch (IOException ex) {
          // Ignore errors when closing the file
        }
      }
    }
  }
  
  private boolean isCryptoMaterialSet(Configuration conf, String username) {
    for (HopsSSLSocketFactory.CryptoKeys key : HopsSSLSocketFactory.CryptoKeys.values()) {
      String propValue = conf.get(key.getValue(), key.getDefaultValue());
      if (checkForDefaultInProperty(key, propValue)
        || !checkUsernameInProperty(username, propValue, key.getType())) {
        return false;
      }
    }
    
    return true;
  }
  
  private boolean checkForDefaultInProperty(HopsSSLSocketFactory.CryptoKeys key, String propValue) {
    if (key.getType() != HopsSSLSocketFactory.PropType.LITERAL) {
      if (key.getDefaultValue().equals(propValue)) {
        return true;
      }
    }
    
    return false;
  }
  
  private boolean checkUsernameInProperty(String username, String propValue, HopsSSLSocketFactory.PropType propType) {
    if (propType == HopsSSLSocketFactory.PropType.FILEPATH) {
      return propValue.contains(username);
    }
    
    return true;
  }
  
  private boolean isHostnameInCryptoMaterial(String hostname, Configuration configuration) {
    for (HopsSSLSocketFactory.CryptoKeys key : HopsSSLSocketFactory.CryptoKeys.values()) {
      String propValue = configuration.get(key.getValue(), key.getDefaultValue());
      if (key.getType() == HopsSSLSocketFactory.PropType.FILEPATH
        && !propValue.contains(hostname)) {
        return false;
      }
    }
    return true;
  }
}
