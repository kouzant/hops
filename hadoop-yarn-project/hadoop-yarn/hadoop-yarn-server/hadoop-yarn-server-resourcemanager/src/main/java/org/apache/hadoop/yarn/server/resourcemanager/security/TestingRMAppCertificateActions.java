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

import org.bouncycastle.asn1.x500.X500NameBuilder;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequest;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Security;
import java.security.cert.X509Certificate;
import java.util.Date;

public class TestingRMAppCertificateActions implements RMAppCertificateActions {
  
  private final static String KEY_ALGORITHM = "RSA";
  private final static String SIGNATURE_ALGORITHM = "SHA256withRSA";
  private final static int KEY_SIZE = 1024;
  
  private final KeyPair caKeyPair;
  private final X509Certificate caCert;
  private final ContentSigner sigGen;
  
  public TestingRMAppCertificateActions() throws Exception {
    Security.addProvider(new BouncyCastleProvider());
    KeyPairGenerator kpg = KeyPairGenerator.getInstance(KEY_ALGORITHM, "BC");
    kpg.initialize(KEY_SIZE);
    caKeyPair = kpg.genKeyPair();
  
    X500NameBuilder subjectBuilder = new X500NameBuilder(BCStyle.INSTANCE);
    subjectBuilder.addRDN(BCStyle.CN, "RootCA");
  
    sigGen = new JcaContentSignerBuilder(SIGNATURE_ALGORITHM).setProvider("BC").build(caKeyPair
        .getPrivate());
    X509v3CertificateBuilder certGen = new JcaX509v3CertificateBuilder(subjectBuilder.build(),
        BigInteger.ONE, new Date(), new Date(System.currentTimeMillis() + 600000),
        subjectBuilder.build(), caKeyPair.getPublic());
    caCert = new JcaX509CertificateConverter().setProvider("BC").getCertificate(certGen.build(sigGen));
    
    caCert.checkValidity();
    caCert.verify(caKeyPair.getPublic());
    caCert.verify(caCert.getPublicKey());
  }
  
  public X509Certificate getCaCert() {
    return caCert;
  }
  
  @Override
  public X509Certificate sign(PKCS10CertificationRequest csr) throws URISyntaxException, IOException, GeneralSecurityException {
    JcaPKCS10CertificationRequest jcaRequest = new JcaPKCS10CertificationRequest(csr);
    X509v3CertificateBuilder certBuilder = new JcaX509v3CertificateBuilder(caCert,
        BigInteger.valueOf(System.currentTimeMillis()),
        new Date(), new Date(System.currentTimeMillis() + 50000),
        csr.getSubject(),
        jcaRequest.getPublicKey());
  
    JcaX509ExtensionUtils extensionUtils = new JcaX509ExtensionUtils();
    certBuilder
        .addExtension(Extension.authorityKeyIdentifier, false, extensionUtils.createAuthorityKeyIdentifier(caCert))
        .addExtension(Extension.subjectKeyIdentifier, false, extensionUtils
            .createSubjectKeyIdentifier(jcaRequest.getPublicKey()))
        .addExtension(Extension.basicConstraints, true, new BasicConstraints(0))
        .addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment));
    
    return new JcaX509CertificateConverter().setProvider("BC")
        .getCertificate(certBuilder.build(sigGen));
  }
}
