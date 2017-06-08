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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.ssl.CertificateLocalization;
import org.apache.hadoop.security.ssl.CryptoMaterial;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.RMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.api.CertificateLocalizationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.MaterializeCryptoKeysRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.MaterializeCryptoKeysResponse;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
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

public class CertificateLocalizationService extends AbstractService
    implements CertificateLocalization, CertificateLocalizationProtocol {
  
  private final Logger LOG = LogManager.getLogger
      (CertificateLocalizationService.class);
  
  private final String SYSTEM_TMP = System.getProperty("java.io.tmpdir",
      "/tmp");
  private final String LOCALIZATION_DIR = "certLoc";
  private Path materializeDir;
  
  private final Map<StorageKey, CryptoMaterial> materialLocation =
      new ConcurrentHashMap<>();
  private final Map<StorageKey, Future<CryptoMaterial>> futures =
      new ConcurrentHashMap<>();
  private final ExecutorService execPool = Executors.newFixedThreadPool(5);
  private final boolean isLeader;
  private final boolean isHAEnabled;
  
  private File tmpDir;
  
  private InetSocketAddress resourceManagerAddress;
  private Server server;
  private RecordFactory recordFactory;
  private CertificateLocalizationProtocol localizationProtocol;
  
  public CertificateLocalizationService(boolean isLeader, boolean isHAEnabled) {
    super(CertificateLocalizationService.class.getName());
    this.isLeader = isLeader;
    this.isHAEnabled = isHAEnabled;
  }
  
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    /*resourceManagerAddress = conf.getSocketAddr(YarnConfiguration.RM_BIND_HOST,
        YarnConfiguration.RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);*/
    resourceManagerAddress = conf.getSocketAddr(YarnConfiguration.RM_BIND_HOST,
        "rm.certificate.localizer.address",
        "0.0.0.0:8812",
        8812);
    recordFactory = RecordFactoryProvider.getRecordFactory(conf);
    
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
    LOG.debug("Initialized at dir: " + materializeDir.toString());
    
    if (isHAEnabled) {
      if (isLeader) {
        startSyncClient();
      } else {
        startSyncService();
      }
    }
    super.serviceStart();
  }
  
  private void startSyncService() {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);
    this.server = rpc.getServer(CertificateLocalizationProtocol.class, this,
        resourceManagerAddress, conf, null, 20);
    this.server.start();
  }
  
  private void startSyncClient() {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(getConfig());
    localizationProtocol = (CertificateLocalizationProtocol) rpc.getProxy
        (CertificateLocalizationProtocol.class,
            resourceManagerAddress, conf);
  }
  
  @Override
  protected void serviceStop() throws Exception {
    if (null != this.server) {
      this.server.stop();
    }
    
    if (null != materializeDir) {
      FileUtils.deleteQuietly(materializeDir.toFile());
    }
    
    LOG.debug("Stopped CertificateLocalization service");
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
  public void materializeCertificates(String username,
      ByteBuffer keyStore, ByteBuffer trustStore) throws IOException {
    StorageKey key = new StorageKey(username);
    CryptoMaterial material = materialLocation.get(key);
    if (null != material) {
      material.incrementRequestedApplications();
      return;
    }
  
    Future<CryptoMaterial> future = execPool.submit(new Materializer(key,
        keyStore, trustStore));
    futures.put(key, future);
    // Put the CryptoMaterial lazily in the materialLocation map
  
    LOG.debug("Materializing for user " + username + " kstore: " +
        keyStore.capacity() + " tstore: " + trustStore.capacity());
  }
  
  @Override
  public void removeMaterial(String username)
      throws InterruptedException, ExecutionException {
    StorageKey key = new StorageKey(username);
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
    
    material.decrementRequestedApplications();
    
    if (material.isSafeToRemove()) {
      execPool.execute(new Remover(key, material));
      LOG.debug("Removing crypto material for user " + key.getUsername());
    } else {
      LOG.debug("There are " + material.getRequestedApplications()
          + " applications using the crypto material. " +
          "They will not be removed now!");
    }
  }
  
  @Override
  public CryptoMaterial getMaterialLocation(String username)
      throws FileNotFoundException, InterruptedException, ExecutionException {
    StorageKey key = new StorageKey(username);
  
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
  
  // CertificateLocalizationService RPC
  @Override
  public MaterializeCryptoKeysResponse materializeCrypto(
      MaterializeCryptoKeysRequest request) throws YarnException, IOException {
    LOG.error("Received *materializeCrypto* request " + request);
    MaterializeCryptoKeysResponse response = recordFactory.newRecordInstance
        (MaterializeCryptoKeysResponse.class);
    
    try {
      materializeCertificates(request.getUsername(), request.getKeystore(),
          request.getTruststore());
      response.setSuccess(true);
    } catch (IOException ex) {
      response.setSuccess(false);
    }
    
    return response;
  }
  
  
  private class StorageKey {
    private final String username;
    
    public StorageKey(String username) {
      this.username = username;
    }
    
    public String getUsername() {
      return username;
    }
    
    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      }
      
      if (!(other instanceof StorageKey)) {
        return false;
      }
      
      return username.equals(((StorageKey) other).getUsername());
    }
    
    @Override
    public int hashCode() {
      int result = 17;
      result = 31 * result + username.hashCode();
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
      File appDir = Paths.get(materializeDir.toString(), key.getUsername())
          .toFile();
      if (!appDir.exists()) {
        appDir.mkdir();
      }
      File kstoreFile = Paths.get(appDir.getAbsolutePath(),
          key.getUsername() + "__kstore.jks").toFile();
      File tstoreFile = Paths.get(appDir.getAbsolutePath(),
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
      futures.remove(key);
      
      if (null != localizationProtocol) {
        MaterializeCryptoKeysRequest request = Records.newRecord
            (MaterializeCryptoKeysRequest.class);
        request.setUsername(key.getUsername());
        request.setKeystore(kstore);
        request.setTruststore(tstore);
        try {
          MaterializeCryptoKeysResponse response = localizationProtocol
              .materializeCrypto(request);
          if (!response.getSuccess()) {
            LOG.error("Could not sync crypto material");
          }
        } catch (YarnException ex) {
          LOG.error("Error while syncing crypto material: " + ex, ex);
        }
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
      File appDir = Paths.get(materializeDir.toString(), key.getUsername())
          .toFile();
      FileUtils.deleteQuietly(appDir);
      materialLocation.remove(key);
    }
  }
}
