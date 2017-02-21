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
package org.apache.hadoop.security.ssl;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;

public class TestCertificateLocalizer {
  private final Logger LOG = LogManager.getLogger(TestCertificateLocalizer.class);
  @Test
  public void testInitialization() throws Exception {
    CertificateLocalizer loc = CertificateLocalizer.getInstance();
    Path dir = loc.getMatDir();
    File fd = dir.toFile();
    String[] files = fd.list();
    LOG.error("Listing files " + files.length);
    for (String file : files) {
      LOG.error("File: " + file);
    }
  }
  
  @Test
  public void testCryptoMaterial() throws Exception {
    CertificateLocalizer loc = CertificateLocalizer.getInstance();
    Path materDir = loc.getMatDir();
    byte[] randomK = "Some_random_keystore_stuff".getBytes();
    byte[] randomT = "Some_random_truststore_stuff".getBytes();
    ByteBuffer kstore = ByteBuffer.wrap(randomK);
    ByteBuffer tstore = ByteBuffer.wrap(randomT);
    loc.materializeCertificates("antonis", kstore, tstore);
    CertificateLocalizer.CryptoMaterial material = loc.getMaterialLocation
        ("antonis");
    
    assertEquals(materDir.toAbsolutePath() + "/antonis__kstore.jks",
        material.getKeyStoreLocation());
    assertEquals(materDir.toAbsolutePath() + "/antonis__tstore.jks",
        material.getTrustStoreLocation());
    
    File tmp = new File("/tmp/certLoc");
    String[] files = tmp.list();
    LOG.error("Listing files: " + files.length);
    for (String file : files) {
      LOG.error("File: " + file);
    }
  }
}
