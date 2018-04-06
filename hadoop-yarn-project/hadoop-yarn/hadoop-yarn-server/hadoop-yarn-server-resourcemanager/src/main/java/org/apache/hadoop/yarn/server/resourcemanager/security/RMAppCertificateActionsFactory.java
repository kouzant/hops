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

import org.apache.hadoop.conf.Configuration;

public final class RMAppCertificateActionsFactory {
  
  private final Configuration conf;
  
  private static RMAppCertificateActionsFactory _INSTANCE = null;
  
  private RMAppCertificateActions actor;
  
  private RMAppCertificateActionsFactory(Configuration conf) {
    this.conf = conf;
  }
  
  public static RMAppCertificateActionsFactory getInstance(Configuration conf) {
    if (_INSTANCE == null) {
      synchronized (RMAppCertificateActionsFactory.class) {
        if (_INSTANCE == null) {
          _INSTANCE = new RMAppCertificateActionsFactory(conf);
        }
      }
    }
    
    return _INSTANCE;
  }
  
  public RMAppCertificateActions getActor() throws Exception {
    if (actor == null) {
      synchronized (RMAppCertificateActionsFactory.class) {
        if (actor == null) {
          actor = new HopsworksRMAppCertificateActions(conf);
        }
      }
    }
    return actor;
  }
  
  // This is for testing
  public void register(RMAppCertificateActions actor) {
    this.actor = actor;
  }
  
  public void clean() {
    this.actor = null;
  }
}
