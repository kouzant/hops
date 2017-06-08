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
package org.apache.hadoop.yarn.server.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ssl.CertificateLocalization;
import org.apache.hadoop.security.ssl.CryptoMaterial;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

public class TestCertificateLocalizationService {
  
  private CertificateLocalizationService certLocSrv;
  private Configuration conf;
  
  @Rule
  public final ExpectedException rule = ExpectedException.none();
  
  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    certLocSrv = new CertificateLocalizationService(false, false);
    certLocSrv.serviceInit(conf);
    certLocSrv.serviceStart();
  }
  
  @After
  public void tearDown() throws Exception {
    if (null != certLocSrv) {
      certLocSrv.serviceStop();
      File fd = certLocSrv.getMaterializeDirectory().toFile();
      assertFalse(fd.exists());
    }
  }
  
  @Test
  public void testMaterialSyncService() throws Exception {
    // Stop the Service started without HA
    if (null != certLocSrv) {
      certLocSrv.serviceStop();
    }
    
    CertificateLocalizationService certSyncLeader = new CertificateLocalizationService
        (true, true);
    certSyncLeader.serviceInit(conf);
    certSyncLeader.serviceStart();
    
    CertificateLocalizationService certSyncSlave = new
        CertificateLocalizationService(false, true);
    certSyncSlave.serviceInit(conf);
    certSyncSlave.serviceStart();
    
    String username = "Dr.Who";
    ByteBuffer kstore = ByteBuffer.wrap("some bytes".getBytes());
    ByteBuffer tstore = ByteBuffer.wrap("some bytes".getBytes());
    certSyncLeader.materializeCertificates(username, kstore, tstore);
    
    TimeUnit.SECONDS.sleep(2);
    
    String slaveCertLoc = certSyncSlave.getMaterializeDirectory().toString();
    String expectedKPath = Paths.get(slaveCertLoc, username, username +
        "__kstore.jks").toString();
    String expectedTPath = Paths.get(slaveCertLoc, username, username +
        "__tstore.jks").toString();
    CryptoMaterial material = certSyncSlave.getMaterialLocation(username);
    assertEquals(expectedKPath, material.getKeyStoreLocation());
    assertEquals(expectedTPath, material.getTrustStoreLocation());
    
    certSyncLeader.removeMaterial(username);
    
    TimeUnit.SECONDS.sleep(2);
    
    File kfd = new File(expectedKPath);
    File tfd = new File(expectedTPath);
    
    assertFalse(kfd.exists());
    assertFalse(tfd.exists());
    
    certSyncSlave.serviceStop();
    certSyncLeader.serviceStop();
  }
  
  @Test
  public void testLocalizationDirectory() {
    File tmpDir = certLocSrv.getTmpDir();
    assertTrue(tmpDir.exists());
    assertTrue(tmpDir.canWrite());
    assertTrue(tmpDir.canExecute());
    assertFalse(tmpDir.canRead());
    
    Path materializeDir = certLocSrv.getMaterializeDirectory();
    File fd = materializeDir.toFile();
    assertTrue(fd.exists());
    assertTrue(fd.canWrite());
    assertTrue(fd.canExecute());
    assertTrue(fd.canRead());
  }
  
  @Test
  public void testMaterialization() throws Exception {
    byte[] randomK = "Some_random_keystore_stuff".getBytes();
    byte[] randomT = "Some_random_truststore_stuff".getBytes();
    ByteBuffer bfk = ByteBuffer.wrap(randomK);
    ByteBuffer bft = ByteBuffer.wrap(randomT);
    String username = "Dr.Who";
    String applicationId = "tardis";
    
    certLocSrv.materializeCertificates(username, bfk, bft);
  
    CryptoMaterial cryptoMaterial = certLocSrv
        .getMaterialLocation(username);
    String materializeDir = certLocSrv.getMaterializeDirectory().toString();
    String expectedKPath = Paths.get(materializeDir, username, username
        + "__kstore.jks").toString();
    String expectedTPath = Paths.get(materializeDir, username, username
        + "__tstore.jks").toString();
    
    assertEquals(expectedKPath, cryptoMaterial.getKeyStoreLocation());
    assertEquals(expectedTPath, cryptoMaterial.getTrustStoreLocation());
    
    cryptoMaterial = certLocSrv.getMaterialLocation(username);
    assertEquals(expectedKPath, cryptoMaterial.getKeyStoreLocation());
    assertEquals(expectedTPath, cryptoMaterial.getTrustStoreLocation());
    
    certLocSrv.removeMaterial(username);
    File kfd = new File(expectedKPath);
    File tfd = new File(expectedTPath);
  
    // Deletion is asynchronous so we have to wait
    TimeUnit.MILLISECONDS.sleep(10);
    assertFalse(kfd.exists());
    assertFalse(tfd.exists());
  }
  
  @Test
  public void testMaterializationWithMultipleApplications() throws Exception {
    byte[] randomK = "Some_random_keystore_stuff".getBytes();
    byte[] randomT = "Some_random_truststore_stuff".getBytes();
    ByteBuffer bfk = ByteBuffer.wrap(randomK);
    ByteBuffer bft = ByteBuffer.wrap(randomT);
    String username = "Dr.Who";
    String applicationId = "tardis";
  
    certLocSrv.materializeCertificates(username, bfk, bft);
  
    CryptoMaterial cryptoMaterial = certLocSrv
        .getMaterialLocation(username);
    String materializeDir = certLocSrv.getMaterializeDirectory().toString();
    String expectedKPath = Paths.get(materializeDir, username, username
        + "__kstore.jks").toString();
    String expectedTPath = Paths.get(materializeDir, username, username
        + "__tstore.jks").toString();
  
    assertEquals(expectedKPath, cryptoMaterial.getKeyStoreLocation());
    assertEquals(expectedTPath, cryptoMaterial.getTrustStoreLocation());
  
    cryptoMaterial = certLocSrv.getMaterialLocation(username);
    assertEquals(expectedKPath, cryptoMaterial.getKeyStoreLocation());
    assertEquals(expectedTPath, cryptoMaterial.getTrustStoreLocation());
    
    // Make a second materialize certificates call which happen when a second
    // application is launched
    certLocSrv.materializeCertificates(username, bfk, bft);
    cryptoMaterial = certLocSrv.getMaterialLocation(username);
    assertEquals(2, cryptoMaterial.getRequestedApplications());
    
    certLocSrv.removeMaterial(username);
    TimeUnit.MILLISECONDS.sleep(10);
    cryptoMaterial = certLocSrv.getMaterialLocation(username);
    assertEquals(1, cryptoMaterial.getRequestedApplications());
    assertFalse(cryptoMaterial.isSafeToRemove());
  
    File kfd = new File(expectedKPath);
    File tfd = new File(expectedTPath);
    assertTrue(kfd.exists());
    assertTrue(tfd.exists());
  
    certLocSrv.removeMaterial(username);
    TimeUnit.MILLISECONDS.sleep(10);
    
    rule.expect(FileNotFoundException.class);
    certLocSrv.getMaterialLocation(username);
    
    assertFalse(kfd.exists());
    assertFalse(tfd.exists());
  }
  
  @Test
  public void testMaterialNotFound() throws Exception {
    rule.expect(FileNotFoundException.class);
    certLocSrv.getMaterialLocation("username");
  }
}
