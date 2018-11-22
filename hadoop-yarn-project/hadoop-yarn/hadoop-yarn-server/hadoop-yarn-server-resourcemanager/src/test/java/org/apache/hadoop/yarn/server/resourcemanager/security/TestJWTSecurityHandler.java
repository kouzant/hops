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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class TestJWTSecurityHandler extends RMSecurityHandlersBaseTest {
  private static final Log LOG = LogFactory.getLog(TestJWTSecurityHandler.class);
  
  private Configuration config;
  private DrainDispatcher dispatcher;
  private RMContext rmContext;
  
  @Before
  public void beforeTest() {
    config = new Configuration();
    config.setBoolean(YarnConfiguration.RM_JWT_ENABLED, true);
    
    RMAppSecurityActionsFactory.getInstance().clear();
    dispatcher = new DrainDispatcher();
    rmContext = new RMContextImpl(dispatcher, null, null, null, null, null, null, null, null);
    dispatcher.init(config);
    dispatcher.start();
  }
  
  @After
  public void afterTest() {
    if (dispatcher != null) {
      dispatcher.stop();
    }
  }
  
  @Test
  public void testJWTGenerationEvent() throws Exception {
    config.set(YarnConfiguration.HOPS_RM_SECURITY_ACTOR_KEY,
        "org.apache.hadoop.yarn.server.resourcemanager.security.TestingRMAppSecurityActions");
    
    MockRMAppEventHandler eventHandler = new MockRMAppEventHandler(RMAppEventType.SECURITY_MATERIAL_GENERATED);
    rmContext.getDispatcher().register(RMAppEventType.class, eventHandler);
    
    RMAppSecurityManager securityManager = new RMAppSecurityManager(rmContext);
    JWTSecurityHandler jwtHandler = Mockito.spy(new MockJWTSecurityHandler(rmContext, securityManager));
    securityManager.registerRMAppSecurityHandlerWithType(jwtHandler, JWTSecurityHandler.class);
    securityManager.init(config);
    securityManager.start();
  
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    JWTSecurityHandler.JWTMaterialParameter jwtParam =
        new JWTSecurityHandler.JWTMaterialParameter(appId, "Alice");
    RMAppSecurityMaterial securityMaterial = new RMAppSecurityMaterial();
    securityMaterial.addMaterial(jwtParam);
    RMAppSecurityManagerEvent event = new RMAppSecurityManagerEvent(appId, securityMaterial,
        RMAppSecurityManagerEventType.GENERATE_SECURITY_MATERIAL);
    securityManager.handle(event);
    
    dispatcher.await();
    eventHandler.verifyEvent();
    securityManager.stop();
    Mockito.verify(jwtHandler).generateMaterial(Mockito.eq(jwtParam));
  }
  
  @Test
  public void testJWTRevocation() throws Exception {
    config.set(YarnConfiguration.HOPS_RM_SECURITY_ACTOR_KEY,
        "org.apache.hadoop.yarn.server.resourcemanager.security.TestingRMAppSecurityActions");
    RMAppSecurityActions actor = Mockito.spy(new TestingRMAppSecurityActions());
    RMAppSecurityActionsFactory.getInstance().register(actor);
    
    RMAppSecurityManager securityManager = new RMAppSecurityManager(rmContext);
    JWTSecurityHandler jwtHandler = Mockito.spy(new MockJWTSecurityHandler(rmContext, securityManager));
    // Block Invalidation handler thread by acquiring the semaphore
    ((MockJWTSecurityHandler)jwtHandler).getSemaphore().acquire();
    
    securityManager.registerRMAppSecurityHandlerWithType(jwtHandler, JWTSecurityHandler.class);
    securityManager.init(config);
    securityManager.start();
    
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    // Create an invalidation event
    JWTSecurityHandler.JWTMaterialParameter param = new JWTSecurityHandler.JWTMaterialParameter(appId, "Alice");
    RMAppSecurityMaterial securityMaterial = new RMAppSecurityMaterial();
    securityMaterial.addMaterial(param);
    
    RMAppSecurityManagerEvent event = new RMAppSecurityManagerEvent(appId, securityMaterial,
        RMAppSecurityManagerEventType.REVOKE_SECURITY_MATERIAL);
    
    // Let security manager handle it
    securityManager.handle(event);
    
    // Verify event is in queue
    JWTSecurityHandler.JWTInvalidationEvent invalidationEvent =
        new JWTSecurityHandler.JWTInvalidationEvent(appId.toString());
    assertTrue(jwtHandler.getInvalidationEvents().contains(invalidationEvent));
    
    // Unblock invalidation handler thread
    ((MockJWTSecurityHandler)jwtHandler).getSemaphore().release();
    
    // Give some time to Invalidation events handler to acquire the lock
    TimeUnit.MILLISECONDS.sleep(10);
    
    // Wait for the event to be processed
    ((MockJWTSecurityHandler)jwtHandler).getSemaphore().acquire();
    
    Mockito.verify(actor).invalidateJWT(Mockito.eq(appId.toString()));
    securityManager.stop();
  }
}
