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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
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
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemWriter;

import java.io.FileWriter;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Security;
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
  
  public RMAppCertificateManager(RMContext rmContext, Configuration conf) throws GeneralSecurityException {
    Security.addProvider(new BouncyCastleProvider());
    this.rmContext = rmContext;
    this.conf = conf;
    this.handler = rmContext.getDispatcher().getEventHandler();
    this.certificateLocalizationService = rmContext.getCertificateLocalizationService();
    keyPairGenerator = KeyPairGenerator.getInstance(KEY_ALGORITHM, SECURITY_PROVIDER);
    keyPairGenerator.initialize(KEY_SIZE);
  }
  
  // Scope is protected to ease testing
  @SuppressWarnings("unchecked")
  protected void generateCertificate(ApplicationId appId, String appUser) {
    try {
      PKCS10CertificationRequest csr = generateKeysAndCSR(appId, appUser);
      // TODO(Antonis): Send CSR for signing
      
      // TODO(Antonis): Construct keystore and truststore
      
      // TODO(Antonis): Send them along with the START event
      handler.handle(new RMAppEvent(appId, RMAppEventType.START));
    } catch (OperatorCreationException ex) {
      LOG.error("Error while generating certificate for application " + appId);
      handler.handle(new RMAppEvent(appId, RMAppEventType.KILL, "Error while generating application certificate"));
    }
  }
  
  // Scope is protected to ease testing
  protected PKCS10CertificationRequest generateKeysAndCSR(ApplicationId appId, String applicationUser)
      throws OperatorCreationException {
    LOG.info("Generating certificate for application: " + appId);
    // 1) Generate key-pair
    KeyPair kp = generateKeyPair();
    // 2) Create X500 subject CN=APPLICATION_ID, O=USER
    X500Name subject = createX500Subject(appId, applicationUser);
    // 3) Create Certificate Signing Request
    return createCSR(subject, kp);
  }
  
  private KeyPair generateKeyPair() {
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
}
