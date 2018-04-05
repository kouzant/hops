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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.junit.Assert;
import org.junit.Test;

import java.security.GeneralSecurityException;

public class TestRMAppCertificateManager {
  private static final Log LOG = LogFactory.getLog(TestRMAppCertificateManager.class);
  
  @Test
  public void testCSRCreation() throws Exception {
    Dispatcher dispatcher = new AsyncDispatcher();
    RMContext rmContext = new RMContextImpl(dispatcher, null, null, null, null, null, null, null, null);
    Configuration conf = new Configuration();
    MockRMAppCertificateManager manager = new MockRMAppCertificateManager(rmContext, conf);
    manager.handle(new RMAppCertificateManagerEvent(
        ApplicationId.newInstance(System.currentTimeMillis(), 1),
        "userA",
        RMAppCertificateManagerEventType.GENERATE_CERTIFICATE));
  }
  
  private class MockRMAppCertificateManager extends RMAppCertificateManager {
  
    public MockRMAppCertificateManager(RMContext rmContext,
        Configuration conf) throws GeneralSecurityException {
      super(rmContext, conf);
    }
    
    @Override
    protected void generateCertificate(ApplicationId applicationId, String appUser) {
      boolean exceptionThrown = false;
      try {
        PKCS10CertificationRequest csr = generateKeysAndCSR(applicationId, appUser);
        String[] subjectTokens = csr.getSubject().toString().split(",", 2);
        Assert.assertEquals(2, subjectTokens.length);
        String cn = subjectTokens[0].split("=", 2)[1];
        Assert.assertEquals(appUser, cn);
        String o = subjectTokens[1].split("=", 2)[1];
        Assert.assertEquals(applicationId.toString(), o);
      } catch (Exception ex) {
        LOG.error(ex);
        exceptionThrown = true;
      }
      Assert.assertFalse(exceptionThrown);
    }
  }
}
