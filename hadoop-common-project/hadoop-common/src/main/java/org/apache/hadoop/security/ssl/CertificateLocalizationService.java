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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class CertificateLocalizationService extends AbstractService
    implements CertificateLocalization {
  private final Logger LOG = LogManager.getLogger
      (CertificateLocalizationService.class);
  
  private final String SYSTEM_TMP = System.getProperty("java.io.tmpdir",
      "/tmp");
  private final String LOCALIZATION_DIR = "certLoc";
  private Path materializeDir;
  
  private final Map<StorageKey, CryptoMaterial> materialLocation =
      new ConcurrentHashMap<>();
  private final Map<String, Set<CryptoMaterial>> usernameToCryptoMaterial =
      new ConcurrentHashMap<>();
  private final Map<StorageKey, Future<CryptoMaterial>> futures =
      new ConcurrentHashMap<>();
  private final ExecutorService execPool = Executors.newFixedThreadPool(5);
  
  private File tmpDir;
  
  public CertificateLocalizationService() {
    super(CertificateLocalizationService.class.getName());
  }
  
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // TODO Get the localization directory from conf, for the moment is a
    // random UUID
    super.serviceInit(conf);
  }
  
  @Override
  protected void serviceStart() throws Exception {
    String uuid = UUID.randomUUID().toString();
    tmpDir = Paths.get(SYSTEM_TMP, LOCALIZATION_DIR).toFile();
    if (!tmpDir.exists()) {
      tmpDir.mkdir();
    }
    tmpDir.setExecutable(false, false);
    tmpDir.setExecutable(true);
    // Writable only to the owner
    tmpDir.setWritable(false, false);
    tmpDir.setWritable(true);
    // Readable by none
    tmpDir.setReadable(false, false);
    
    materializeDir = Paths.get(tmpDir.getAbsolutePath(), uuid);
    // Random materialization directory should have the default umask
    materializeDir.toFile().mkdir();
    LOG.error("Initialized at dir: " + materializeDir.toString());
    
    super.serviceStart();
  }
  
  @Override
  protected void serviceStop() throws Exception {
    if (null != materializeDir) {
      FileUtils.deleteQuietly(materializeDir.toFile());
    }
    
    super.serviceStop();
  }
  
  @VisibleForTesting
  public Path getMaterializeDirectory() {
    return materializeDir;
  }
  
  @VisibleForTesting
  public File getTmpDir() {
    return tmpDir;
  }
  
  @Override
  public void materializeCertificates(String username, String applicationId,
      ByteBuffer keyStore, ByteBuffer trustStore) throws IOException {
    StorageKey key = new StorageKey(username, applicationId);
    if (materialLocation.containsKey(key)) {
      return;
    }
  
    Future<CryptoMaterial> future = execPool.submit(new Materializer(key,
        keyStore, trustStore));
    futures.put(key, future);
    // Put the CryptoMaterial lazily in the materialLocation map
  
    LOG.error("<kavouri> Materializing for user " + username + "kstore: " +
        keyStore.capacity() + " tstore: " + trustStore.capacity());
  }
  
  @Override
  public void removeMaterial(String username, String applicationId)
      throws InterruptedException, ExecutionException {
    StorageKey key = new StorageKey(username, applicationId);
    CryptoMaterial material = null;
  
    Future<CryptoMaterial> future = futures.remove(key);
    if (future != null) {
      material = future.get();
    } else {
      material = materialLocation.get(key);
    }
  
    if (null == material) {
      LOG.warn("Certificates do not exists for user " + username);
      return;
    }
    execPool.execute(new Remover(key, material));
  }
  
  @Override
  // HopSSLSocketFactory has no notion of Application ID
  public CryptoMaterial getMaterialLocation(
      String username) throws FileNotFoundException {
    Set<CryptoMaterial> userMaterial = usernameToCryptoMaterial.get
        (username);
    if (userMaterial == null || userMaterial.isEmpty()) {
      throw new FileNotFoundException("Materialized crypto material for user "
          + username + " could not be found");
    }
  
    // Get the first available CryptoMaterial for that user
    Iterator<CryptoMaterial> iter = userMaterial.iterator();
    return iter.next();
  }
  
  @Override
  public CryptoMaterial getMaterialLocation(
      String username, String applicationId)
      throws FileNotFoundException, InterruptedException, ExecutionException {
    StorageKey key = new StorageKey(username, applicationId);
  
    CryptoMaterial material = null;
    Future<CryptoMaterial> future = futures.remove(key);
  
    // There is an async operation for this username
    if (future != null) {
      material = future.get();
    } else {
      // Materialization has already been finished
      material = materialLocation.get(key);
    }
  
    if (material == null) {
      throw new FileNotFoundException("Materialized crypto material could not" +
          " be found");
    }
  
    return material;
  }
  
  public class CryptoMaterial {
    private final String keyStoreLocation;
    private final int keyStoreSize;
    private final String trustStoreLocation;
    private final int trustStoreSize;
    private final ByteBuffer keyStoreMem;
    private final ByteBuffer trustStoreMem;
    
    public CryptoMaterial(String keyStoreLocation, String trustStoreLocation,
        ByteBuffer kStore, ByteBuffer tstore) {
      this.keyStoreLocation = keyStoreLocation;
      this.keyStoreSize = kStore.capacity();
      this.trustStoreLocation = trustStoreLocation;
      this.trustStoreSize = tstore.capacity();
      
      this.keyStoreMem = kStore.asReadOnlyBuffer();
      this.trustStoreMem = tstore.asReadOnlyBuffer();
    }
    
    public String getKeyStoreLocation() {
      return keyStoreLocation;
    }
    
    public int getKeyStoreSize() {
      return keyStoreSize;
    }
    
    public String getTrustStoreLocation() {
      return trustStoreLocation;
    }
    
    public int getTrustStoreSize() {
      return trustStoreSize;
    }
    
    public ByteBuffer getKeyStoreMem() {
      return keyStoreMem;
    }
    
    public ByteBuffer getTrustStoreMem() {
      return trustStoreMem;
    }
  }
  
  private class StorageKey {
    private final String username;
    private final String applicationId;
    
    public StorageKey(String username, String applicationId) {
      this.username = username;
      this.applicationId = applicationId;
    }
    
    public String getUsername() {
      return username;
    }
    
    public String getApplicationId() {
      return applicationId;
    }
    
    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      }
      
      if (!(other instanceof StorageKey)) {
        return false;
      }
      
      StorageKey otherKey = (StorageKey) other;
      
      return Objects.equals(username, otherKey.username)
          && Objects.equals(applicationId, otherKey.applicationId);
      
    }
    
    @Override
    public int hashCode() {
      int result = 17;
      result = 31 * result + username.hashCode();
      result = 31 * result + applicationId.hashCode();
      return result;
    }
  }
  
  private class Materializer implements Callable<CryptoMaterial> {
    private final StorageKey key;
    private final ByteBuffer kstore;
    private final ByteBuffer tstore;
    
    private Materializer(StorageKey key, ByteBuffer kstore, ByteBuffer tstore) {
      this.key = key;
      this.kstore = kstore;
      this.tstore = tstore;
    }
    
    @Override
    public CryptoMaterial call() throws IOException {
      File appDir = Paths.get(materializeDir.toString(), key.getApplicationId())
          .toFile();
      if (!appDir.exists()) {
        appDir.mkdir();
      }
      File kstoreFile = Paths.get(materializeDir.toString(), key.getApplicationId(),
          key.getUsername() + "__kstore.jks").toFile();
      File tstoreFile = Paths.get(materializeDir.toString(), key.getApplicationId(),
          key.getUsername() + "__tstore.jks").toFile();
      FileChannel kstoreChannel = new FileOutputStream(kstoreFile, false)
          .getChannel();
      FileChannel tstoreChannel = new FileOutputStream(tstoreFile, false)
          .getChannel();
      kstoreChannel.write(kstore);
      tstoreChannel.write(tstore);
      kstoreChannel.close();
      tstoreChannel.close();
      
      CryptoMaterial material = new CryptoMaterial(kstoreFile.getAbsolutePath(),
          tstoreFile.getAbsolutePath(), kstore, tstore);
      materialLocation.put(key, material);
      
      Set<CryptoMaterial> materialForUser = usernameToCryptoMaterial.get(key
          .getUsername());
      if (null != materialForUser) {
        materialForUser.add(material);
      } else {
        Set<CryptoMaterial> tmp = new HashSet<>();
        tmp.add(material);
        usernameToCryptoMaterial.put(key.getUsername(), tmp);
      }
      return material;
    }
  }
  
  private class Remover implements Runnable {
    private final StorageKey key;
    private final CryptoMaterial material;
    
    private Remover(StorageKey key, CryptoMaterial material) {
      this.key = key;
      this.material = material;
    }
    
    @Override
    public void run() {
      /*FileUtils.deleteQuietly(new File(material.getKeyStoreLocation()));
      FileUtils.deleteQuietly(new File(material.getTrustStoreLocation()));*/
      File appDir = Paths.get(materializeDir.toString(), key.getApplicationId())
          .toFile();
      FileUtils.deleteQuietly(appDir);
      Set<CryptoMaterial> userMaterial = usernameToCryptoMaterial.get(key
          .getUsername());
      if (null != userMaterial) {
        userMaterial.remove(material);
        if (userMaterial.isEmpty()) {
          usernameToCryptoMaterial.remove(key.getUsername());
        }
      }
      materialLocation.remove(key);
    }
  }
}
