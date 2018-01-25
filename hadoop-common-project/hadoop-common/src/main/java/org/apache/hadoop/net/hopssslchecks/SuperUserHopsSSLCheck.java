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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.HopsSSLSocketFactory;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.ssl.CertificateLocalization;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Set;

public class SuperUserHopsSSLCheck extends AbstractHopsSSLCheck {
  private final static Log LOG = LogFactory.getLog(SuperUserHopsSSLCheck.class);
  
  public SuperUserHopsSSLCheck() {
    super(-1);
  }
  
  @Override
  public HopsSSLCryptoMaterial check(String username, Set<String> proxySuperUsers, Configuration configuration,
      CertificateLocalization certificateLocalization) throws IOException, SSLMaterialAlreadyConfiguredException {
    
    if (proxySuperUsers.contains(username)) {
      String hostname = NetUtils.getLocalHostname();
      isConfigurationNeededForSuperUser(username, hostname, configuration);
      
      // TODO(Antonis) Maybe I don't need to check if the keystore exists, just try to read it from ssl-server.xml
      String serviceCertificateDir = configuration.get(HopsSSLSocketFactory.CryptoKeys.SERVICE_CERTS_DIR.getValue(),
          HopsSSLSocketFactory.CryptoKeys.SERVICE_CERTS_DIR.getDefaultValue());
      File fd = Paths.get(serviceCertificateDir, hostname + HopsSSLSocketFactory.KEYSTORE_SUFFIX).toFile();
      if (fd.exists()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Found crypto material with the hostname");
        }
        
        if (certificateLocalization != null) {
          return new HopsSSLCryptoMaterial(
              certificateLocalization.getSuperKeystoreLocation(),
              certificateLocalization.getSuperKeystorePass(),
              certificateLocalization.getSuperTruststoreLocation(),
              certificateLocalization.getSuperTruststorePass());
        }
        
        if (LOG.isDebugEnabled()) {
          LOG.debug("*** Called setTlsConfiguration for superuser but CertificateLocalization is NULL");
        }
        
        return readSuperuserMaterialFromFile(configuration);
      }
    }
    
    return null;
  }
}
