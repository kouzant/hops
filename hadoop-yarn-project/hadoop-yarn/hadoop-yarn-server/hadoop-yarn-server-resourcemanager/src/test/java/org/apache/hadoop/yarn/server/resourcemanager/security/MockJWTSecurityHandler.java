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
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;

import java.time.Instant;
import java.time.temporal.TemporalUnit;
import java.util.concurrent.Semaphore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MockJWTSecurityHandler extends JWTSecurityHandler {
  private final Log LOG = LogFactory.getLog(MockJWTSecurityHandler.class);
  private Instant now;
  private final Semaphore semaphore;
  
  public MockJWTSecurityHandler(RMContext rmContext, RMAppSecurityManager rmAppSecurityManager) {
    super(rmContext, rmAppSecurityManager);
    semaphore = new Semaphore(1);
  }
  
  @Override
  public JWTSecurityManagerMaterial generateMaterial(JWTMaterialParameter parameter) throws Exception {
    if (!isJWTEnabled()) {
      return null;
    }
    ApplicationId appId = parameter.getApplicationId();
    now = Instant.now();
    prepareJWTGenerationParameters(parameter);
    assertTrue(now.isBefore(parameter.getExpirationDate()));
    Pair<Long, TemporalUnit> validity = getValidityPeriod();
    Instant expTime = now.plus(validity.getFirst(), validity.getSecond());
    assertEquals(parameter.getExpirationDate(), expTime);
    assertFalse(parameter.isRenewable());
    String jwt = generateInternal(parameter);
    assertNotNull(jwt);
    assertFalse(jwt.isEmpty());
    return new JWTSecurityManagerMaterial(appId, jwt, parameter.getExpirationDate());
  }
  
  @Override
  protected Instant getNow() {
    return now;
  }
  
  @Override
  protected Thread createInvalidationEventsHandler() {
    return new BlockingInvalidationEventHandler();
  }
  
  public Semaphore getSemaphore() {
    return semaphore;
  }
  
  private Pair<Long, TemporalUnit> getValidityPeriod() {
    return getRmAppSecurityManager().parseInterval(
        getConfig().get(YarnConfiguration.RM_JWT_VALIDITY_PERIOD,
            YarnConfiguration.DEFAULT_RM_JWT_VALIDITY_PERIOD),
        YarnConfiguration.RM_JWT_VALIDITY_PERIOD);
  }
  
  protected class BlockingInvalidationEventHandler extends InvalidationEventsHandler {
    @Override
    public void run() {
      try {
        // One shot
        semaphore.acquire();
        JWTInvalidationEvent event = getInvalidationEvents().take();
        revokeInternal(event.getSigningKeyName());
        semaphore.release();
      } catch (InterruptedException ex) {
        LOG.error("It should not have blocked here", ex);
        Thread.currentThread().interrupt();
      }
    }
  }
}
