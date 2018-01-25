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
package org.apache.hadoop.net.hopssslchecks;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.HopsSSLSocketFactory;

import java.io.IOException;

public abstract class AbstractHopsSSLCheck implements HopsSSLCheck, Comparable<HopsSSLCheck> {
  private final Integer priority;
  
  public AbstractHopsSSLCheck(Integer priority) {
    this.priority = priority;
  }
  
  public abstract HopsSSLCryptoMaterial check(Configuration configuration) throws IOException;
  
  @Override
  public Integer getPriority() {
    return priority;
  }
  
  @Override
  public int compareTo(HopsSSLCheck hopsSSLCheck) {
    return priority.compareTo(hopsSSLCheck.getPriority());
  }
  
  protected boolean isCryptoMaterialSet(Configuration conf, String username) {
    for (HopsSSLSocketFactory.CryptoKeys key : HopsSSLSocketFactory.CryptoKeys.values()) {
      String prop
    }
  }
}
