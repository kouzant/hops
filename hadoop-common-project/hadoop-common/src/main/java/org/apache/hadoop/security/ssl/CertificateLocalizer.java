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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.util.ShutdownHookManager;
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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class CertificateLocalizer {
  private final Logger LOG = LogManager.getLogger(CertificateLocalizer.class);
  
  private static volatile CertificateLocalizer instance = null;
  
  private final String TMP_DIR = "/tmp/certLoc";
  private File tmpDir;
  private final Map<String, CryptoMaterial> materialLocation;
  private final ExecutorService execPool;
  private final Map<String, Future<CryptoMaterial>> futures;
  
  private Path materializeDir;
  
  private CertificateLocalizer() {
    materialLocation = new ConcurrentHashMap<>();
    execPool = Executors.newFixedThreadPool(5);
    futures = new ConcurrentHashMap<>();
    
    String uuid = UUID.randomUUID().toString();
    tmpDir = new File(TMP_DIR);
    if (!tmpDir.exists()) {
      tmpDir.mkdir();
    }
    
    materializeDir = Paths.get(TMP_DIR, uuid);
    final File fd = materializeDir.toFile();
    fd.mkdir();
    LOG.error("Initialized at dir: " + fd.getAbsolutePath());
    //tmpDir.setExecutable(false, false);
    ShutdownHookManager.get().addShutdownHook(new Runnable() {
      @Override
      public void run() {
        FileUtils.deleteQuietly(fd);
      }
    }, 1);
  }
  
  public static CertificateLocalizer getInstance() {
    if (instance == null) {
      synchronized (CertificateLocalizer.class) {
        if (instance == null) {
          instance = new CertificateLocalizer();
        }
      }
    }
    return instance;
  }
  
  public Path getMatDir() {
    return materializeDir;
  }
  
  public void materializeCertificates(String username, ByteBuffer keyStore,
      ByteBuffer trustStore) throws IOException {
    if (materialLocation.containsKey(username)) {
      return;
    }
    
    //tmpDir.setExecutable(true, true);
    Future<CryptoMaterial> future = execPool.submit(new Materializer(username,
        keyStore, trustStore));
    futures.put(username, future);
    //tmpDir.setExecutable(false, false);
    // Put the CryptoMaterial lazily in the materialLocation map
    
    LOG.error("<kavouri> Materializing for user " + username + "kstore: " +
        keyStore.capacity() + " tstore: " + trustStore.capacity());
  }
  
  public CryptoMaterial getMaterialLocation(String username) throws
      FileNotFoundException, InterruptedException, ExecutionException {
    CryptoMaterial material = null;
    
    Future<CryptoMaterial> future = futures.remove(username);
    
    // There is an async operation for this username
    if (future != null) {
      material = future.get();
      materialLocation.put(username, material);
    } else {
      // Materialization has already been finished
      material = materialLocation.get(username);
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
  
  private class Materializer implements Callable<CryptoMaterial> {
    private final String username;
    private final ByteBuffer kstore;
    private final ByteBuffer tstore;
    
    public Materializer(String username, ByteBuffer kstore, ByteBuffer tstore) {
      this.username = username;
      this.kstore = kstore;
      this.tstore = tstore;
    }
    
    @Override
    public CryptoMaterial call() throws IOException {
      File kstoreFile = Paths.get(materializeDir.toString(), username +
          "__kstore.jks").toFile();
      File tstoreFile = Paths.get(materializeDir.toString(), username +
          "__tstore.jks").toFile();
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
      
      return material;
    }
  }
}
