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
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.ssl.CertificateLocalization;
import org.apache.hadoop.security.ssl.CryptoMaterial;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.api.CertificateLocalizationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.MaterializeCryptoKeysRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.MaterializeCryptoKeysResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveCryptoKeysRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveCryptoKeysResponse;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

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
  private final boolean isHAEnabled;
  private final List<CertificateLocalizationProtocol> clients = new
      ArrayList<>();
  private BlockingQueue<CertificateLocalizationEvent> evtQueue = null;
  private Thread eventProcessor = null;
  private volatile boolean stopped = false;
  
  private File tmpDir;
  
  private Server server;
  private RecordFactory recordFactory;
  
  public CertificateLocalizationService(boolean isHAEnabled) {
    super(CertificateLocalizationService.class.getName());
    this.isHAEnabled = isHAEnabled;
  }
  
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    if (isHAEnabled) {
      recordFactory = RecordFactoryProvider.getRecordFactory(conf);
    }
    
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
    
    super.serviceStart();
  }
  
  public void transitionToActive() {
    if (isHAEnabled) {
      LOG.info("Transitioned to active");
      stopServer();
      stopClients();
      startSyncClients();
      if (null != evtQueue) {
        evtQueue.clear();
      }
      evtQueue = new LinkedBlockingQueue<>();
      eventProcessor = new Thread(new EventProcessor());
      eventProcessor.setDaemon(true);
      eventProcessor.setName("CertificateLocalizationService - EvtProcessor");
      eventProcessor.start();
    }
  }
  
  public void transitionToStandby() {
    if (isHAEnabled) {
      LOG.info("Transitioned to standby");
      stopServer();
      stopClients();
      startSyncService();
      if (null != evtQueue) {
        evtQueue.clear();
        evtQueue = null;
      }
      if (null != eventProcessor) {
        eventProcessor.interrupt();
        stopped = true;
        eventProcessor = null;
      }
    }
  }
  
  private void stopClients() {
    for (CertificateLocalizationProtocol client : clients) {
      RPC.stopProxy(client);
    }
    clients.clear();
  }
  
  private void stopServer() {
    if (null != this.server) {
      this.server.stop();
      this.server = null;
    }
  }
  
  private void startSyncService() {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);
    String rmId = conf.get(YarnConfiguration.RM_HA_ID);
    InetSocketAddress resourceManagerAddress =
        conf.getSocketAddr(YarnConfiguration.RM_BIND_HOST,
            YarnConfiguration.RM_HA_CERT_LOC_ADDRESS + "." + rmId,
            YarnConfiguration.DEFAULT_RM_HA_CERT_LOC_ADDRESS,
            YarnConfiguration.DEFAULT_CERTIFICATE_LOCALIZER_PORT);
    this.server = rpc.getServer(CertificateLocalizationProtocol.class, this,
        resourceManagerAddress, conf, null, 3);
    this.server.start();
  }
  
  private void startSyncClients() {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(getConfig());
    
    for (String addr : getStandbyRMs(conf)) {
      InetSocketAddress socket = NetUtils.createSocketAddr(addr);
      CertificateLocalizationProtocol locProt =
          (CertificateLocalizationProtocol) rpc.getProxy
              (CertificateLocalizationProtocol.class, socket, conf);
      clients.add(locProt);
    }
  }
  
  private List<String> getStandbyRMs(Configuration conf) {
    Collection<String> rmIds = conf.getStringCollection(YarnConfiguration
        .RM_HA_IDS);
    String myId = HAUtil.getRMHAId(conf);
    List<String> rmAddresses = new ArrayList<>(rmIds.size() - 1);
    for (String rmId : rmIds) {
      if (!rmId.equals(myId)) {
        String address = conf.get(YarnConfiguration.RM_HA_CERT_LOC_ADDRESS + "." +
            rmId);
        if (null != address) {
          rmAddresses.add(address);
        }
      }
    }
    return rmAddresses;
  }
  
  @Override
  protected void serviceStop() throws Exception {
    stopServer();
    stopClients();
    
    if (null != materializeDir) {
      FileUtils.deleteQuietly(materializeDir.toFile());
    }
    
    if (null != eventProcessor) {
      eventProcessor.interrupt();
      stopped = true;
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
    LOG.debug("Received *materializeCrypto* request " + request);
    MaterializeCryptoKeysResponse response = recordFactory.newRecordInstance
        (MaterializeCryptoKeysResponse.class);
    
    try {
      materializeCertificates(request.getUsername(), request.getKeystore(),
          request.getTruststore());
      response.setSuccess(true);
    } catch (IOException ex) {
      response.setSuccess(false);
      LOG.error("Could not sync crypto material materialization " + ex, ex);
    }
    
    return response;
  }
  
  // CertificateLocalizationService RPC
  @Override
  public RemoveCryptoKeysResponse removeCrypto(RemoveCryptoKeysRequest request)
      throws YarnException, IOException {
    LOG.debug("Received *removeCrypto* request " + request);
    RemoveCryptoKeysResponse response = recordFactory.newRecordInstance
        (RemoveCryptoKeysResponse.class);
    
    try {
      removeMaterial(request.getUsername());
      response.setSuccess(true);
    } catch (InterruptedException | ExecutionException ex) {
      response.setSuccess(false);
      LOG.error("Could not sync crypto material removal " + ex, ex);
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

      if (isHAEnabled && eventProcessor != null) {
        MaterializeCryptoKeysRequest request = Records.newRecord
            (MaterializeCryptoKeysRequest.class);
        request.setUsername(key.getUsername());
        request.setKeystore(kstore);
        request.setTruststore(tstore);
        evtQueue.add(new CertificateLocalizationEvent(request));
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
      
      if (isHAEnabled && eventProcessor != null) {
        RemoveCryptoKeysRequest request = Records.newRecord
            (RemoveCryptoKeysRequest.class);
        request.setUsername(key.username);
        evtQueue.add(new CertificateLocalizationEvent(request));
      }
    }
  }
  
  private class CertificateLocalizationEvent<T> {
    private final T request;
    
    private CertificateLocalizationEvent(T request) {
      this.request = request;
    }
    
    private T getRequest() {
      return request;
    }
  }
  
  private class EventProcessor implements Runnable {
    
    private EventProcessor() {
      super();
    }
    
    @Override
    public void run() {
      while (!stopped && !Thread.currentThread().isInterrupted()) {
        try {
          CertificateLocalizationEvent evt = evtQueue.take();
          if (evt.getRequest() instanceof MaterializeCryptoKeysRequest) {
            MaterializeCryptoKeysRequest request =
                (MaterializeCryptoKeysRequest) evt.getRequest();
            materializeRequest(request);
          } else if (evt.getRequest() instanceof RemoveCryptoKeysRequest) {
            RemoveCryptoKeysRequest request = (RemoveCryptoKeysRequest) evt
                .getRequest();
            removeRequest(request);
          } else {
            LOG.error("Unknown event type");
          }
        } catch (InterruptedException ex) {
          LOG.info("Handler thread has been interrupted");
          Thread.currentThread().interrupt();
          stopped = true;
        }
      }
    }
    
    private void materializeRequest(MaterializeCryptoKeysRequest request) {
      for (CertificateLocalizationProtocol client : clients) {
        try {
          MaterializeCryptoKeysResponse response = client.materializeCrypto
              (request);
          if (!response.getSuccess()) {
            LOG.error("Could sync materialization of crypto material");
          }
        } catch (YarnException | IOException ex) {
          LOG.error("Error while syncing materialization of crypto material: " +
              ex, ex);
        }
      }
    }
    
    private void removeRequest(RemoveCryptoKeysRequest request) {
      for (CertificateLocalizationProtocol client : clients) {
        try {
          RemoveCryptoKeysResponse response = client.removeCrypto(request);
          if (!response.getSuccess()) {
            LOG.error("Could not sync removal of crypto material");
          }
        } catch (YarnException | IOException ex) {
          LOG.error("Error while syncing removal of crypto material: " + ex,
              ex);
        }
      }
    }
  }
}
