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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppCertificateGeneratedEvent;
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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
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
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class RMAppCertificateManager extends AbstractService
    implements EventHandler<RMAppCertificateManagerEvent> {
  private final static Log LOG = LogFactory.getLog(RMAppCertificateManager.class);
  private final static String SECURITY_PROVIDER = "BC";
  private final static String KEY_ALGORITHM = "RSA";
  private final static String SIGNATURE_ALGORITHM = "SHA256withRSA";
  private final static int KEY_SIZE = 1024;
  private final static int REVOCATION_QUEUE_SIZE = 100;
  
  private final SecureRandom rng;
  private final String TMP = System.getProperty("java.io.tmpdir");
  private final BlockingQueue<CertificateRevocationEvent> revocationEvents;
  
  private RMContext rmContext;
  private Configuration conf;
  private EventHandler handler;
  private CertificateLocalizationService certificateLocalizationService;
  private KeyPairGenerator keyPairGenerator;
  private RMAppCertificateActions rmAppCertificateActions;
  private Thread revocationEventsHandler;
  private boolean isRPCTLSEnabled = false;
  
  private final int RENEWER_THREAD_POOL = 5;
  private final ScheduledExecutorService scheduler;
  private final Map<ApplicationId, ScheduledFuture> renewalTasks;
  
  public RMAppCertificateManager(RMContext rmContext) {
    super(RMAppCertificateManager.class.getName());
    Security.addProvider(new BouncyCastleProvider());
    this.rmContext = rmContext;
    rng = new SecureRandom();
    revocationEvents = new ArrayBlockingQueue<CertificateRevocationEvent>(REVOCATION_QUEUE_SIZE);
    renewalTasks = new ConcurrentHashMap<>();
    scheduler = Executors.newScheduledThreadPool(RENEWER_THREAD_POOL,
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("X509 app certificate renewal thread #%d")
            .build());
  }
  
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    LOG.debug("Initializing RMAppCertificateManager");
    this.conf = conf;
    this.handler = rmContext.getDispatcher().getEventHandler();
    this.certificateLocalizationService = rmContext.getCertificateLocalizationService();
    rmAppCertificateActions = RMAppCertificateActionsFactory.getInstance().getActor(conf);
    keyPairGenerator = KeyPairGenerator.getInstance(KEY_ALGORITHM, SECURITY_PROVIDER);
    keyPairGenerator.initialize(KEY_SIZE);
    isRPCTLSEnabled = conf.getBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED,
        CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED_DEFAULT);
    super.serviceInit(conf);
  }
  
  @Override
  protected void serviceStart() throws Exception {
    LOG.info("Starting RMAppCertificateManager");
    revocationEventsHandler = new RevocationEventsHandler();
    revocationEventsHandler.setDaemon(false);
    revocationEventsHandler.setName("RevocationEventsHandler");
    revocationEventsHandler.start();
    super.serviceStart();
  }
  
  @Override
  protected void serviceStop() throws Exception {
    LOG.info("Stopping RMAppCertificateManager");
    if (revocationEventsHandler != null) {
      revocationEventsHandler.interrupt();
    }
    if (scheduler != null) {
      scheduler.shutdownNow();
    }
  }
  
  @Override
  public void handle(RMAppCertificateManagerEvent event) {
    ApplicationId applicationId = event.getApplicationId();
    LOG.info("Processing event type: " + event.getType() + " for application: " + applicationId);
    if (event.getType().equals(RMAppCertificateManagerEventType.GENERATE_CERTIFICATE)) {
      generateCertificate(applicationId, event.getApplicationUser(), event.getCryptoMaterialVersion());
    } else if (event.getType().equals(RMAppCertificateManagerEventType.REVOKE_CERTIFICATE)) {
      revokeCertificate(applicationId, event.getApplicationUser(), event.getCryptoMaterialVersion());
    } else if (event.getType().equals(RMAppCertificateManagerEventType.REVOKE_GENERATE_CERTIFICATE)) {
      revokeAndGenerateCertificates(applicationId, event.getApplicationUser(), event.getCryptoMaterialVersion());
    } else {
      LOG.warn("Unknown event type " + event.getType());
    }
  }
  
  @VisibleForTesting
  public RMAppCertificateActions getRmAppCertificateActions() {
    return rmAppCertificateActions;
  }
  
  @VisibleForTesting
  protected RMContext getRmContext() {
    return rmContext;
  }
  
  public void registerWithCertificateRenewer(ApplicationId appId, String appUser, Integer currentCryptoVersion,
      long expiration) {
    if (!isRPCTLSEnabled()) {
      return;
    }
    if (!renewalTasks.containsKey(appId)) {
      Instant now = Instant.now();
      Instant expirationInstant = Instant.ofEpochMilli(expiration);
      Instant delay = expirationInstant.minus(now.toEpochMilli(), ChronoUnit.MILLIS)
          .minus(2, ChronoUnit.DAYS);
      ScheduledFuture renewTask = scheduler.schedule(
          new CertificateRenewer(appId, appUser, currentCryptoVersion), delay.toEpochMilli(), TimeUnit.MILLISECONDS);
      renewalTasks.put(appId, renewTask);
    }
  }
  
  public void unregisterFromCertificateRenewer(ApplicationId appId) {
    if (!isRPCTLSEnabled()) {
      return;
    }
    ScheduledFuture task = renewalTasks.remove(appId);
    if (task != null) {
      task.cancel(true);
    }
  }
  
  @InterfaceAudience.Private
  @VisibleForTesting
  public void revokeAndGenerateCertificates(ApplicationId appId, String appUser, Integer cryptoMaterialVersion) {
    // Certificate revocation here is blocking
    if (revokeInternal(getCertificateIdentifier(appId, appUser, cryptoMaterialVersion))) {
      generateCertificate(appId, appUser, cryptoMaterialVersion);
    } else {
      handler.handle(new RMAppEvent(appId, RMAppEventType.KILL, "Could not revoke previously generated certificate"));
    }
  }
  
  @VisibleForTesting
  @InterfaceAudience.Private
  public boolean isRPCTLSEnabled() {
    return isRPCTLSEnabled;
  }
  
  private class CertificateRenewer implements Runnable {
    private final ApplicationId appId;
    private final String appUser;
    private Integer currentCryptoVersion;
    
    private CertificateRenewer(ApplicationId appId, String appUser, Integer currentCryptoVersion) {
      this.appId = appId;
      this.appUser = appUser;
      this.currentCryptoVersion = currentCryptoVersion;
    }
    
    @Override
    public void run() {
      try {
        KeyPair keyPair = generateKeyPair();
        PKCS10CertificationRequest csr = generateCSR(appId, appUser, keyPair, ++currentCryptoVersion);
        X509Certificate signedCertificate = sendCSRAndGetSigned(csr);
        long expiration = signedCertificate.getNotAfter().getTime();
        
        KeyStoresWrapper keyStoresWrapper = createApplicationStores(signedCertificate, keyPair.getPrivate(), appUser,
            appId);
        byte[] rawProtectedKeyStore = keyStoresWrapper.getRawKeyStore(TYPE.KEYSTORE);
        byte[] rawTrustStore = keyStoresWrapper.getRawKeyStore(TYPE.TRUSTSTORE);
        
        // TODO(Antonis) How to treat new certificates in the CertificateLocalizationService
        
        // TODO(Antonis) Send an event to RMApp to update the crypto material and the version
        
        // TODO(Antonis) Re-register with the renewer
        renewalTasks.remove(appId);
      } catch (Exception ex) {
        // TODO(Antonis) What to do with exceptions?? Send KILL?
      }
    }
  }
  
  // Scope is protected to ease testing
  @InterfaceAudience.Private
  @VisibleForTesting
  @SuppressWarnings("unchecked")
  public void generateCertificate(ApplicationId appId, String appUser, Integer cryptoMaterialVersion) {
    try {
      if (isRPCTLSEnabled()) {
        KeyPair keyPair = generateKeyPair();
        PKCS10CertificationRequest csr = generateCSR(appId, appUser, keyPair, cryptoMaterialVersion);
        X509Certificate signedCertificate = sendCSRAndGetSigned(csr);
        long expirationEpoch = signedCertificate.getNotAfter().getTime();
        
        KeyStoresWrapper keyStoresWrapper = createApplicationStores(signedCertificate, keyPair.getPrivate(), appUser,
            appId);
        byte[] rawProtectedKeyStore = keyStoresWrapper.getRawKeyStore(TYPE.KEYSTORE);
        byte[] rawTrustStore = keyStoresWrapper.getRawKeyStore(TYPE.TRUSTSTORE);
        
        // TODO(Antonis) What should I do with updated certificate version
        rmContext.getCertificateLocalizationService().materializeCertificates(
            appUser, appId.toString(), appUser, ByteBuffer.wrap(rawProtectedKeyStore),
            String.valueOf(keyStoresWrapper.keyStorePassword),
            ByteBuffer.wrap(rawTrustStore), String.valueOf(keyStoresWrapper.trustStorePassword));
        
        handler.handle(new RMAppCertificateGeneratedEvent(
            appId,
            rawProtectedKeyStore, keyStoresWrapper.keyStorePassword,
            rawTrustStore, keyStoresWrapper.trustStorePassword, expirationEpoch));
      } else {
        handler.handle(new RMAppEvent(appId, RMAppEventType.CERTS_GENERATED));
      }
    } catch (Exception ex) {
      LOG.error("Error while generating certificate for application " + appId, ex);
      handler.handle(new RMAppEvent(appId, RMAppEventType.KILL, "Error while generating application certificate"));
    }
  }
  
  @InterfaceAudience.Private
  @VisibleForTesting
  protected X509Certificate sendCSRAndGetSigned(PKCS10CertificationRequest csr)
      throws URISyntaxException, IOException, GeneralSecurityException {
    return rmAppCertificateActions.sign(csr);
  }

  @InterfaceAudience.Private
  @VisibleForTesting
  protected PKCS10CertificationRequest generateCSR(ApplicationId appId, String applicationUser, KeyPair keyPair,
      Integer cryptoMaterialVersion)
      throws OperatorCreationException {
    LOG.info("Generating certificate for application: " + appId);
    // Create X500 subject CN=USER, O=APPLICATION_ID
    X500Name subject = createX500Subject(appId, applicationUser, cryptoMaterialVersion);
    // Create Certificate Signing Request
    return createCSR(subject, keyPair);
  }
  
  @InterfaceAudience.Private
  @VisibleForTesting
  public KeyStore loadSystemTrustStore(Configuration conf) throws GeneralSecurityException, IOException {
    String sslConfName = conf.get(SSLFactory.SSL_SERVER_CONF_KEY, "ssl-server.xml");
    Configuration sslConf = new Configuration();
    sslConf.addResource(sslConfName);
    String trustStoreLocation = sslConf.get(
        FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
            FileBasedKeyStoresFactory.SSL_TRUSTSTORE_LOCATION_TPL_KEY));
    String trustStorePassword = sslConf.get(
        FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
            FileBasedKeyStoresFactory.SSL_TRUSTSTORE_PASSWORD_TPL_KEY));
    String trustStoreType = sslConf.get(
        FileBasedKeyStoresFactory.resolvePropertyName(SSLFactory.Mode.SERVER,
            FileBasedKeyStoresFactory.SSL_TRUSTSTORE_TYPE_TPL_KEY),
        FileBasedKeyStoresFactory.DEFAULT_KEYSTORE_TYPE);
    
    KeyStore trustStore = KeyStore.getInstance(trustStoreType);
    try (FileInputStream fis = new FileInputStream(trustStoreLocation)) {
      trustStore.load(fis, trustStorePassword.toCharArray());
    }
    
    return trustStore;
  }
  
  @InterfaceAudience.Private
  @VisibleForTesting
  protected KeyStoresWrapper createApplicationStores(X509Certificate certificate, PrivateKey privateKey,
      String appUser, ApplicationId appId)
      throws GeneralSecurityException, IOException {
    char[] password = generateRandomPassword();
    
    KeyStore keyStore = KeyStore.getInstance("JKS");
    keyStore.load(null, null);
    X509Certificate[] chain = new X509Certificate[1];
    chain[0] = certificate;
    keyStore.setKeyEntry(appUser, privateKey, password, chain);
    
    KeyStore systemTrustStore = loadSystemTrustStore(conf);
    KeyStore appTrustStore = KeyStore.getInstance("JKS");
    appTrustStore.load(null, null);
    
    Enumeration<String> aliases = systemTrustStore.aliases();
    while (aliases.hasMoreElements()) {
      String alias = aliases.nextElement();
      X509Certificate cert = (X509Certificate) systemTrustStore.getCertificate(alias);
      appTrustStore.setCertificateEntry(alias, cert);
    }
    
    return new KeyStoresWrapper(keyStore, password, appTrustStore, password, appUser, appId);
  }
  
  @InterfaceAudience.Private
  @VisibleForTesting
  protected KeyPair generateKeyPair() {
    return keyPairGenerator.genKeyPair();
  }
  
  public char[] generateRandomPassword() {
    return RandomStringUtils.random(20, 0, 0, true, true, null, rng)
        .toCharArray();
  }
  
  private X500Name createX500Subject(ApplicationId appId, String applicationUser, Integer cryptoMaterialVersion) {
    if (appId == null || applicationUser == null) {
      throw new IllegalArgumentException("ApplicationID and application user cannot be null");
    }
    X500NameBuilder x500NameBuilder = new X500NameBuilder(BCStyle.INSTANCE);
    x500NameBuilder.addRDN(BCStyle.CN, applicationUser);
    x500NameBuilder.addRDN(BCStyle.O, appId.toString());
    x500NameBuilder.addRDN(BCStyle.OU, cryptoMaterialVersion.toString());
    return x500NameBuilder.build();
  }
  
  private PKCS10CertificationRequest createCSR(X500Name subject, KeyPair keyPair) throws OperatorCreationException {
    PKCS10CertificationRequestBuilder csrBuilder = new JcaPKCS10CertificationRequestBuilder(
        subject, keyPair.getPublic());
    return csrBuilder.build(
        new JcaContentSignerBuilder(SIGNATURE_ALGORITHM).setProvider(SECURITY_PROVIDER).build(keyPair.getPrivate()));
  }
  
  @InterfaceAudience.Private
  @VisibleForTesting
  public void revokeCertificate(ApplicationId appId, String applicationUser, Integer cryptoMaterialVersion) {
    if (isRPCTLSEnabled()) {
      LOG.info("Revoking certificate for application: " + appId);
      try {
        putToQueue(appId, applicationUser, cryptoMaterialVersion);
        if (certificateLocalizationService != null) {
          certificateLocalizationService.removeMaterial(applicationUser, appId.toString());
        }
      } catch (InterruptedException ex) {
        LOG.warn("Could not remove material for user " + applicationUser + " and application " + appId, ex);
      }
    }
  }
  
  @InterfaceAudience.Private
  @VisibleForTesting
  protected void putToQueue(ApplicationId appId, String applicationUser, Integer cryptoMaterialVersion)
      throws InterruptedException {
    revocationEvents.put(new CertificateRevocationEvent(getCertificateIdentifier(appId, applicationUser, cryptoMaterialVersion)));
  }
  
  // Used only for testing
  @VisibleForTesting
  protected void waitForQueueToDrain() throws InterruptedException {
    while (revocationEvents.peek() != null) {
      TimeUnit.MILLISECONDS.sleep(10);
    }
  }
  
  private boolean revokeInternal(String certificateIdentifier) {
    if (isRPCTLSEnabled()) {
      try {
        rmAppCertificateActions.revoke(certificateIdentifier);
        return true;
      } catch (URISyntaxException | IOException | GeneralSecurityException ex) {
        LOG.error("Could not revoke certificate " + certificateIdentifier, ex);
        return false;
      }
    }
    return true;
  }
  
  private String getCertificateIdentifier(ApplicationId appId, String user, Integer cryptoMaterialVersion) {
    return user + "__" + appId.toString() + "__" + cryptoMaterialVersion;
  }
  
  protected class KeyStoresWrapper {
    private final KeyStore keystore;
    private final char[] keyStorePassword;
    private final KeyStore trustStore;
    private final char[] trustStorePassword;
    private final String appUser;
    private final ApplicationId appId;
    
    private KeyStoresWrapper(KeyStore keyStore, char[] keyStorePassword, KeyStore trustStore, char[] trustStorePassword,
        String appUser, ApplicationId appId) {
      this.keystore = keyStore;
      this.keyStorePassword = keyStorePassword;
      this.trustStore = trustStore;
      this.trustStorePassword = trustStorePassword;
      this.appUser = appUser;
      this.appId = appId;
    }
    
    protected KeyStore getKeystore() {
      return keystore;
    }
    
    protected char[] getKeyStorePassword() {
      return keyStorePassword;
    }
    
    protected KeyStore getTrustStore() {
      return trustStore;
    }
    
    protected char[] getTrustStorePassword() {
      return trustStorePassword;
    }
    
    protected byte[] getRawKeyStore(TYPE type) throws GeneralSecurityException, IOException {
      File target;
      char[] password;
      KeyStore keyStore;
      if (type.equals(TYPE.KEYSTORE)) {
        target = Paths.get(TMP, appUser + "-" + appId.toString() + "_kstore.jks").toFile();
        password = keyStorePassword;
        keyStore = this.keystore;
      } else {
        target = Paths.get(TMP, appUser + "-" + appId.toString() + "_tstore.jks").toFile();
        password = trustStorePassword;
        keyStore = this.trustStore;
      }
      
      try (FileOutputStream fos = new FileOutputStream(target, false)) {
        keyStore.store(fos, password);
      }
      
      byte[] rawKeyStore = Files.readAllBytes(target.toPath());
      FileUtils.deleteQuietly(target);
      return rawKeyStore;
    }
  }
  
  protected enum TYPE {
    KEYSTORE,
    TRUSTSTORE
  }
  
  private class CertificateRevocationEvent {
    private final String identifier;
    
    private CertificateRevocationEvent(String identifier) {
      this.identifier = identifier;
    }
  }
  
  private class RevocationEventsHandler extends Thread {
    
    private void drain() {
      List<CertificateRevocationEvent> events = new ArrayList<>(revocationEvents.size());
      revocationEvents.drainTo(events);
      for (CertificateRevocationEvent event : events) {
        revokeInternal(event.identifier);
      }
    }
    
    @Override
    public void run() {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          CertificateRevocationEvent event = revocationEvents.take();
          revokeInternal(event.identifier);
        } catch (InterruptedException ex) {
          LOG.info("RevocationEventsHandler interrupted. Exiting...");
          drain();
          Thread.currentThread().interrupt();
        }
      }
    }
  }
}
