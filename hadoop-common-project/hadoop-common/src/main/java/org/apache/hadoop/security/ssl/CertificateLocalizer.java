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
import java.util.concurrent.ConcurrentHashMap;

public class CertificateLocalizer {
  private final Logger LOG = LogManager.getLogger(CertificateLocalizer.class);
  
  private static volatile CertificateLocalizer instance = null;
  
  private final String TMP_DIR = "/tmp/certLoc";
  private File tmpDir;
  private final Map<String, CryptoMaterial> materialLocation;
  
  private Path materializeDir;
  
  private CertificateLocalizer() {
    materialLocation = new ConcurrentHashMap<>();
    String uuid = UUID.randomUUID().toString();
    tmpDir = new File(TMP_DIR);
    if (!tmpDir.exists()) {
      tmpDir.mkdir();
    }
    
    materializeDir = Paths.get(TMP_DIR, uuid);
    final File fd = materializeDir.toFile();
    fd.mkdir();
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
    //tmpDir.setExecutable(true, true);
    File kstore = Paths.get(materializeDir.toString(), username +
        "__kstore.jks").toFile();
    File tstore = Paths.get(materializeDir.toString(), username +
        "__tstore.jks").toFile();
    
    FileChannel kstoreChannel = new FileOutputStream(kstore, false)
        .getChannel();
    FileChannel tstoreChannel = new FileOutputStream(tstore, false)
        .getChannel();
    kstoreChannel.write(keyStore);
    tstoreChannel.write(trustStore);
    kstoreChannel.close();
    tstoreChannel.close();
    
    CryptoMaterial material = new CryptoMaterial(kstore.getAbsolutePath(),
        tstore.getAbsolutePath());
    //tmpDir.setExecutable(false, false);
    materialLocation.put(username, material);
  }
  
  public CryptoMaterial getMaterialLocation(String username) throws
      FileNotFoundException {
    CryptoMaterial material = materialLocation.get(username);
    if (material == null) {
      throw new FileNotFoundException("Materialized crypto material could not" +
          " be found");
    }
    
    return material;
  }
  
  public class CryptoMaterial {
    private final String keyStoreLocation;
    private final String trustStoreLocation;
    
    public CryptoMaterial(String keyStoreLocation, String trustStoreLocation) {
      this.keyStoreLocation = keyStoreLocation;
      this.trustStoreLocation = trustStoreLocation;
    }
    
    public String getKeyStoreLocation() {
      return keyStoreLocation;
    }
    
    public String getTrustStoreLocation() {
      return trustStoreLocation;
    }
  }
}
