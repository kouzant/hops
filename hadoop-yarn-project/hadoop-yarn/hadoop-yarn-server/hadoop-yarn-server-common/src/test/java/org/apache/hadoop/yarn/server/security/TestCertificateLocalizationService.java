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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
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
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TestCertificateLocalizationService {
  
  private final Logger LOG = LogManager.getLogger
      (TestCertificateLocalizationService.class);
  private CertificateLocalizationService certLocSrv;
  private Configuration conf;
  
  @Rule
  public final ExpectedException rule = ExpectedException.none();
  
  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    certLocSrv = new CertificateLocalizationService(false);
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
    
    conf.set(YarnConfiguration.RM_HA_ID, "rm0");
    conf.set(YarnConfiguration.RM_HA_IDS, "rm0,rm1,rm2");
    conf.set(YarnConfiguration.RM_HA_CERT_LOC_ADDRESS + ".rm0", "0.0.0.0:8812");
    conf.set(YarnConfiguration.RM_HA_CERT_LOC_ADDRESS + ".rm1", "0.0.0.0:8813");
    conf.set(YarnConfiguration.RM_HA_CERT_LOC_ADDRESS + ".rm2", "0.0.0.0:8814");
    
    CertificateLocalizationService certSyncLeader = new CertificateLocalizationService
        (true);
    certSyncLeader.serviceInit(conf);
    certSyncLeader.serviceStart();
    certSyncLeader.transitionToActive();
    
    Configuration conf2 = new Configuration();
    conf2.set(YarnConfiguration.RM_HA_ID, "rm1");
    conf2.set(YarnConfiguration.RM_HA_IDS, "rm0,rm1,rm2");
    conf2.set(YarnConfiguration.RM_HA_CERT_LOC_ADDRESS + ".rm0",
        "0.0.0.0:8812");
    conf2.set(YarnConfiguration.RM_HA_CERT_LOC_ADDRESS + ".rm1",
        "0.0.0.0:8813");
    conf2.set(YarnConfiguration.RM_HA_CERT_LOC_ADDRESS + ".rm2",
        "0.0.0.0:8814");
    
    CertificateLocalizationService certSyncSlave = new
        CertificateLocalizationService(true);
    certSyncSlave.serviceInit(conf2);
    certSyncSlave.serviceStart();
    certSyncSlave.transitionToStandby();
  
    Configuration conf3 = new Configuration();
    conf3.set(YarnConfiguration.RM_HA_ID, "rm2");
    conf3.set(YarnConfiguration.RM_HA_IDS, "rm0,rm1,rm2");
    conf3.set(YarnConfiguration.RM_HA_CERT_LOC_ADDRESS + ".rm0",
        "0.0.0.0:8812");
    conf3.set(YarnConfiguration.RM_HA_CERT_LOC_ADDRESS + ".rm1",
        "0.0.0.0:8813");
    conf3.set(YarnConfiguration.RM_HA_CERT_LOC_ADDRESS + ".rm2",
        "0.0.0.0:8814");
  
    CertificateLocalizationService certSyncSlave2 = new
        CertificateLocalizationService(true);
    certSyncSlave2.serviceInit(conf3);
    certSyncSlave2.serviceStart();
    certSyncSlave2.transitionToStandby();
    
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
    
    slaveCertLoc = certSyncSlave2.getMaterializeDirectory().toString();
    expectedKPath = Paths.get(slaveCertLoc, username, username +
        "__kstore.jks").toString();
    expectedTPath = Paths.get(slaveCertLoc, username, username +
        "__tstore.jks").toString();
    material = certSyncSlave2.getMaterialLocation(username);
    assertEquals(expectedKPath, material.getKeyStoreLocation());
    assertEquals(expectedTPath, material.getTrustStoreLocation());
    
    certSyncLeader.removeMaterial(username);
    
    TimeUnit.SECONDS.sleep(2);
    
    File kfd = new File(expectedKPath);
    File tfd = new File(expectedTPath);
    
    assertFalse(kfd.exists());
    assertFalse(tfd.exists());
    
    LOG.info("Switching roles");
    // Switch roles
    certSyncSlave.transitionToActive();
    certSyncLeader.transitionToStandby();
    
    username = "Amy";
    certSyncSlave.materializeCertificates(username, kstore, tstore);
    TimeUnit.SECONDS.sleep(2);
    slaveCertLoc = certSyncLeader.getMaterializeDirectory().toString();
    expectedKPath = Paths.get(slaveCertLoc, username, username + "__kstore" +
        ".jks").toString();
    expectedTPath = Paths.get(slaveCertLoc, username, username + "__tstore" +
        ".jks").toString();
    material = certSyncLeader.getMaterialLocation(username);
    assertEquals(expectedKPath, material.getKeyStoreLocation());
    assertEquals(expectedTPath, material.getTrustStoreLocation());
    
    certSyncSlave.removeMaterial(username);
    TimeUnit.SECONDS.sleep(2);
    kfd = new File(expectedKPath);
    tfd = new File(expectedTPath);
    assertFalse(kfd.exists());
    assertFalse(tfd.exists());
    
    certSyncSlave.serviceStop();
    certSyncLeader.serviceStop();
    certSyncSlave2.serviceStop();
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
