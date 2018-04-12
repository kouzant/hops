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
package org.apache.hadoop.yarn.server.resourcemanager.security;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppStartWithCertificateEvent;
import org.apache.hadoop.yarn.server.security.CertificateLocalizationService;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.X500NameBuilder;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.bouncycastle.pkcs.PKCS10CertificationRequestBuilder;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequestBuilder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.X509Certificate;
import java.util.concurrent.ExecutionException;

public class RMAppCertificateManager implements EventHandler<RMAppCertificateManagerEvent> {
  private final static Log LOG = LogFactory.getLog(RMAppCertificateManager.class);
  private final static String SECURITY_PROVIDER = "BC";
  private final static String KEY_ALGORITHM = "RSA";
  private final static String SIGNATURE_ALGORITHM = "SHA256withRSA";
  private final static int KEY_SIZE = 1024;
  
  private final RMContext rmContext;
  private final Configuration conf;
  private final EventHandler handler;
  private final CertificateLocalizationService certificateLocalizationService;
  private final KeyPairGenerator keyPairGenerator;
  private final RMAppCertificateActions rmAppCertificateActions;
  private final SecureRandom rng;
  private final String TMP = System.getProperty("java.io.tmpdir");
  
  public RMAppCertificateManager(RMContext rmContext, Configuration conf) throws Exception {
    Security.addProvider(new BouncyCastleProvider());
    this.rmContext = rmContext;
    this.conf = conf;
    this.handler = rmContext.getDispatcher().getEventHandler();
    this.certificateLocalizationService = rmContext.getCertificateLocalizationService();
    keyPairGenerator = KeyPairGenerator.getInstance(KEY_ALGORITHM, SECURITY_PROVIDER);
    keyPairGenerator.initialize(KEY_SIZE);
    rmAppCertificateActions = RMAppCertificateActionsFactory.getInstance(conf).getActor();
    rng = new SecureRandom();
  }
  
  @Override
  public void handle(RMAppCertificateManagerEvent event) {
    ApplicationId applicationId = event.getApplicationId();
    LOG.info("Processing event type: " + event.getType() + " for application: " + applicationId);
    if (event.getType().equals(RMAppCertificateManagerEventType.GENERATE_CERTIFICATE)) {
      generateCertificate(applicationId, event.getApplicationUser());
    } else if (event.getType().equals(RMAppCertificateManagerEventType.REVOKE_CERTIFICATE)) {
      revokeCertificate(applicationId, event.getApplicationUser());
    } else {
      LOG.warn("Unknown event type " + event.getType());
    }
  }
  
  @VisibleForTesting
  public RMAppCertificateActions getRmAppCertificateActions() {
    return rmAppCertificateActions;
  }
  
  protected RMContext getRmContext() {
    return rmContext;
  }
  
  // Scope is protected to ease testing
  @SuppressWarnings("unchecked")
  protected void generateCertificate(ApplicationId appId, String appUser) {
    try {
      if (conf.getBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED,
          CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED_DEFAULT)) {
        KeyPair keyPair = generateKeyPair();
        PKCS10CertificationRequest csr = generateKeysAndCSR(appId, appUser, keyPair);
        X509Certificate signedCertificate = sendCSRAndGetSigned(csr);
  
        byte[] rawTrustStore = loadRawTrustStore(conf);
        KeyStoreWrapper keyStoreWrapper = createApplicationKeyStore(signedCertificate, keyPair.getPrivate(), appUser,
            appId);
        byte[] rawProtectedKeyStore = keyStoreWrapper.getRawKeyStore();
        
        // TODO(Antonis): Send them along with the START event
        handler.handle(new RMAppStartWithCertificateEvent(
            appId,
            rawProtectedKeyStore, keyStoreWrapper.password,
            rawTrustStore, keyStoreWrapper.password));
      } else {
        handler.handle(new RMAppEvent(appId, RMAppEventType.START));
      }
    } catch (Exception ex) {
      LOG.error("Error while generating certificate for application " + appId);
      handler.handle(new RMAppEvent(appId, RMAppEventType.KILL, "Error while generating application certificate"));
    }
  }
  
  protected X509Certificate sendCSRAndGetSigned(PKCS10CertificationRequest csr)
      throws URISyntaxException, IOException, GeneralSecurityException {
    return rmAppCertificateActions.sign(csr);
  }
  
  // Scope is protected to ease testing
  protected PKCS10CertificationRequest generateKeysAndCSR(ApplicationId appId, String applicationUser, KeyPair keyPair)
      throws OperatorCreationException {
    LOG.info("Generating certificate for application: " + appId);
    // Create X500 subject CN=USER, O=APPLICATION_ID
    X500Name subject = createX500Subject(appId, applicationUser);
    // Create Certificate Signing Request
    return createCSR(subject, keyPair);
  }
  
  protected byte[] loadRawTrustStore(Configuration conf) throws GeneralSecurityException, IOException {
    String sslConfName = conf.get(SSLFactory.SSL_SERVER_CONF_KEY, "ssl-server.xml");
    Configuration sslConf = new Configuration();
    sslConf.addResource(sslConfName);
    String trustStoreLocation = sslConf.get(
        FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
            FileBasedKeyStoresFactory.SSL_TRUSTSTORE_LOCATION_TPL_KEY));
    return Files.readAllBytes(Paths.get(trustStoreLocation));
  }
  
  protected KeyStoreWrapper createApplicationKeyStore(X509Certificate certificate, PrivateKey privateKey,
      String appUser, ApplicationId appId)
    throws GeneralSecurityException, IOException {
    KeyStore keyStore = KeyStore.getInstance("JKS");
    keyStore.load(null, null);
    X509Certificate[] chain = new X509Certificate[1];
    chain[0] = certificate;
    char[] password = RandomStringUtils.random(20, 0, 0, true, true, null, rng)
        .toCharArray();
    
    // TODO (Antonis) Remove it !!!
    LOG.info("Password is: " + String.valueOf(password));
    keyStore.setKeyEntry(appUser, privateKey, password, chain);
    
    return new KeyStoreWrapper(keyStore, password, appUser, appId);
  }
  
  protected KeyPair generateKeyPair() {
    return keyPairGenerator.genKeyPair();
  }
  
  private X500Name createX500Subject(ApplicationId appId, String applicationUser) {
    if (appId == null || applicationUser == null) {
      throw new IllegalArgumentException("ApplicationID and application user cannot be null");
    }
    X500NameBuilder x500NameBuilder = new X500NameBuilder(BCStyle.INSTANCE);
    x500NameBuilder.addRDN(BCStyle.CN, applicationUser);
    x500NameBuilder.addRDN(BCStyle.O, appId.toString());
    return x500NameBuilder.build();
  }
  
  private PKCS10CertificationRequest createCSR(X500Name subject, KeyPair keyPair) throws OperatorCreationException {
    PKCS10CertificationRequestBuilder csrBuilder = new JcaPKCS10CertificationRequestBuilder(
        subject, keyPair.getPublic());
    return csrBuilder.build(
        new JcaContentSignerBuilder(SIGNATURE_ALGORITHM).setProvider(SECURITY_PROVIDER).build(keyPair.getPrivate()));
  }
  
  private void revokeCertificate(ApplicationId appId, String applicationUser) {
    LOG.info("Revoking certificate for application: " + appId);
    if (conf.getBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED,
          CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED_DEFAULT) && certificateLocalizationService != null) {
      try {
        certificateLocalizationService.removeMaterial(applicationUser);
      } catch (InterruptedException | ExecutionException ex) {
        LOG.warn("Could not remove material for user " + applicationUser + " and application " + appId, ex);
      }
    }
  }
  
  protected class KeyStoreWrapper {
    private final KeyStore keystore;
    private final char[] password;
    private final String appUser;
    private final ApplicationId appId;
    
    private KeyStoreWrapper(KeyStore keyStore, char[] password, String appUser, ApplicationId appId) {
      this.keystore = keyStore;
      this.password = password;
      this.appUser = appUser;
      this.appId = appId;
    }
    
    protected KeyStore getKeystore() {
      return keystore;
    }
    
    protected char[] getPassword() {
      return password;
    }
    
    protected byte[] getRawKeyStore() throws GeneralSecurityException, IOException {
      File target = Paths.get(TMP, appUser + "-" + appId.toString() + "_kstore.jks").toFile();
      try (FileOutputStream targetStream = new FileOutputStream(target, false)) {
        keystore.store(targetStream, password);
      }
      byte[] rawKeystore = Files.readAllBytes(target.toPath());
      FileUtils.deleteQuietly(target);
      return rawKeystore;
    }
  }
}
