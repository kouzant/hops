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

import com.google.common.annotations.VisibleForTesting;
import com.nimbusds.jwt.JWT;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.BackOff;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppSecurityMaterialRenewedEvent;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class JWTSecurityHandler
    implements RMAppSecurityHandler<JWTSecurityHandler.JWTSecurityManagerMaterial, JWTSecurityHandler
    .JWTMaterialParameter> {
  private static final Log LOG = LogFactory.getLog(JWTSecurityHandler.class);
  
  private final RMContext rmContext;
  private final RMAppSecurityManager rmAppSecurityManager;
  private final EventHandler eventHandler;
  private final String[] JWT_AUDIENCE;
  
  private Configuration config;
  private boolean jwtEnabled;
  private RMAppSecurityActions rmAppSecurityActions;
  private Pair<Long, TemporalUnit> validityPeriod;
  private final Map<ApplicationId, ScheduledFuture> renewalTasks;
  private ScheduledExecutorService renewalExecutorService;
  
  private Thread invalidationEventsHandler;
  private static final int INVALIDATION_EVENTS_QUEUE_SIZE = 100;
  private final BlockingQueue<JWTInvalidationEvent> invalidationEvents;
  
  public JWTSecurityHandler(RMContext rmContext, RMAppSecurityManager rmAppSecurityManager) {
    this.rmContext = rmContext;
    this.rmAppSecurityManager = rmAppSecurityManager;
    
    // TODO(Antonis): Maybe configurable???
    JWT_AUDIENCE = new String[]{"job"};
    
    this.renewalTasks = new ConcurrentHashMap<>();
    this.invalidationEvents = new ArrayBlockingQueue<JWTInvalidationEvent>(INVALIDATION_EVENTS_QUEUE_SIZE);
    this.eventHandler = rmContext.getDispatcher().getEventHandler();
  }
  
  @Override
  public void init(Configuration config) throws Exception {
    LOG.info("Initializing JWT Security Handler");
    this.config = config;
    jwtEnabled = config.getBoolean(YarnConfiguration.RM_JWT_ENABLED,
        YarnConfiguration.DEFAULT_RM_JWT_ENABLED);
  
    renewalExecutorService = rmAppSecurityManager.getRenewalExecutorService();
    // TODO(Antonis): Configurable
    String validity = "30m";
    validityPeriod = rmAppSecurityManager.parseInterval(validity, "I-have-to-fix-it");
    if (jwtEnabled) {
      rmAppSecurityActions = RMAppSecurityActionsFactory.getInstance().getActor(config);
    }
  }
  
  @Override
  public void start() throws Exception {
    LOG.info("Starting JWT Security Handler");
    if (isJWTEnabled()) {
      invalidationEventsHandler = new InvalidationEventsHandler();
      invalidationEventsHandler.setDaemon(false);
      invalidationEventsHandler.setName("JWT-InvalidationEventsHandler");
      invalidationEventsHandler.start();
    }
  }
  
  @Override
  public void stop() throws Exception {
    LOG.info("Stopping JWT Security Handler");
    if (invalidationEventsHandler != null) {
      invalidationEventsHandler.interrupt();
    }
  }
  
  @Override
  public JWTSecurityManagerMaterial generateMaterial(JWTMaterialParameter parameter) throws Exception {
    if (!isJWTEnabled()) {
      return null;
    }
    ApplicationId appId = parameter.getApplicationId();
    prepareJWTGenerationParameters(parameter);
    String jwt = generateInternal(parameter);
    return new JWTSecurityManagerMaterial(appId, jwt, parameter.getExpirationDate());
  }
  
  private void prepareJWTGenerationParameters(JWTMaterialParameter parameter) {
    parameter.setAudiences(JWT_AUDIENCE);
    Instant now = Instant.now();
    Instant expirationInstant = now.plus(validityPeriod.getFirst(), validityPeriod.getSecond());
    Instant renewNotBefore = expirationInstant.plus(1L, ChronoUnit.HOURS);
    parameter.setExpirationDate(expirationInstant);
    parameter.setRenewNotBefore(Date.from(renewNotBefore));
    // JWT for applications will not be automatically renewed.
    // JWTSecurityHandler will renew them
    parameter.setRenewable(false);
    parameter.setExpLeeway(-1);
  }
  
  private String generateInternal(JWTMaterialParameter parameter) throws URISyntaxException, IOException {
    return rmAppSecurityActions.generateJWT(parameter);
  }
  
  @Override
  public void registerRenewer(JWTMaterialParameter parameter) {
    if (!isJWTEnabled()) {
      return;
    }
    if (!renewalTasks.containsKey(parameter.getApplicationId())) {
      Instant now = Instant.now();
      Instant delay = parameter.getExpirationDate()
          .minus(now.toEpochMilli(), ChronoUnit.MILLIS)
          // TODO(Antonis) This should be configurable
          .minus(10L, ChronoUnit.MINUTES);
      
      ScheduledFuture task = renewalExecutorService.schedule(
          createJWTRenewalTask(parameter.getApplicationId(), parameter.appUser),
          delay.toEpochMilli(), TimeUnit.MILLISECONDS);
      renewalTasks.put(parameter.getApplicationId(), task);
    }
  }
  
  public void deregisterFromRenewer(ApplicationId appId) {
    if (!isJWTEnabled()) {
      return;
    }
    ScheduledFuture task = renewalTasks.get(appId);
    if (task != null) {
      task.cancel(true);
    }
  }
  
  private Runnable createJWTRenewalTask(ApplicationId appId, String appUser) {
    return new JWTRenewer(appId, appUser);
  }
  
  @Override
  public boolean revokeMaterial(JWTMaterialParameter parameter, Boolean blocking) {
    // Return value does not matter for JWT
    if (!isJWTEnabled()) {
      return true;
    }
    ApplicationId appId = parameter.getApplicationId();
    try {
      LOG.info("Invalidating JWT for application: " + appId);
      deregisterFromRenewer(appId);
      putToInvalidationQueue(appId);
      return true;
    } catch (InterruptedException ex) {
      LOG.warn("Shutting down while putting invalidation event to queue for application " + appId);
    }
    return false;
  }
  
  private void putToInvalidationQueue(ApplicationId appId) throws InterruptedException {
    invalidationEvents.put(new JWTInvalidationEvent(appId.toString()));
  }
  
  private void revokeInternal(String signingKeyName) {
    if (!isJWTEnabled()) {
      return;
    }
    try {
      rmAppSecurityActions.invalidateJWT(signingKeyName);
    } catch (URISyntaxException | IOException ex) {
      LOG.error("Could not invalidate JWT with signing key " + signingKeyName, ex);
    }
  }
  
  private boolean isJWTEnabled() {
    return jwtEnabled;
  }
  
  public class JWTSecurityManagerMaterial extends RMAppSecurityManager.SecurityManagerMaterial {
    private final String token;
    private final Instant expirationDate;
    
    public JWTSecurityManagerMaterial(ApplicationId applicationId, String token, Instant expirationDate) {
      super(applicationId);
      this.token = token;
      this.expirationDate = expirationDate;
    }
    
    public String getToken() {
      return token;
    }
    
    public Instant getExpirationDate() {
      return expirationDate;
    }
  }
  
  public static class JWTMaterialParameter extends RMAppSecurityManager.SecurityManagerMaterial {
    private final String appUser;
    private String[] audiences;
    private Instant expirationDate;
    private Date renewNotBefore;
    private boolean renewable;
    private int expLeeway;
    
    public JWTMaterialParameter(ApplicationId applicationId, String appUser) {
      super(applicationId);
      this.appUser = appUser;
    }
    
    public String getAppUser() {
      return appUser;
    }
  
    public String[] getAudiences() {
      return audiences;
    }
  
    public void setAudiences(String[] audiences) {
      this.audiences = audiences;
    }
  
    public Instant getExpirationDate() {
      return expirationDate;
    }
  
    public void setExpirationDate(Instant expirationDate) {
      this.expirationDate = expirationDate;
    }
  
    public Date getRenewNotBefore() {
      return renewNotBefore;
    }
  
    public void setRenewNotBefore(Date renewNotBefore) {
      this.renewNotBefore = renewNotBefore;
    }
  
    public boolean isRenewable() {
      return renewable;
    }
  
    public void setRenewable(boolean renewable) {
      this.renewable = renewable;
    }
  
    public int getExpLeeway() {
      return expLeeway;
    }
  
    public void setExpLeeway(int expLeeway) {
      this.expLeeway = expLeeway;
    }
  }
  
  @VisibleForTesting
  @InterfaceAudience.Private
  protected class JWTRenewer implements Runnable {
    private final ApplicationId appId;
    private final String appUser;
    private final BackOff backOff;
    private long backOffTime = 0L;
    
    public JWTRenewer(ApplicationId appId, String appUser) {
      this.appId = appId;
      this.appUser = appUser;
      this.backOff = rmAppSecurityManager.createBackOffPolicy();
    }
  
    @Override
    public void run() {
      try {
        LOG.debug("Renewing JWT for application " + appId);
        JWTMaterialParameter jwtParam = new JWTMaterialParameter(appId, appUser);
        prepareJWTGenerationParameters(jwtParam);
        String jwt = generateInternal(jwtParam);
        renewalTasks.remove(appId);
        JWTSecurityManagerMaterial jwtMaterial = new JWTSecurityManagerMaterial(appId, jwt,
            jwtParam.getExpirationDate());
        
        eventHandler.handle(new RMAppSecurityMaterialRenewedEvent<>(appId, jwtMaterial));
        LOG.debug("Renewed JWT for application " + appId);
      } catch (Exception ex) {
        LOG.error(ex, ex);
        renewalTasks.remove(appId);
        backOffTime = backOff.getBackOffInMillis();
        if (backOffTime != -1) {
          LOG.warn("Failed to renew JWT for application " + appId + ". Retrying in " + backOffTime + " ms");
          ScheduledFuture task = renewalExecutorService.schedule(this, backOffTime, TimeUnit.MILLISECONDS);
          renewalTasks.put(appId, task);
        } else {
          LOG.error("Failed to renew JWT for application " + appId + ". Failed more than 4 times, giving up");
        }
      }
    }
  }
  
  private class JWTInvalidationEvent {
    private final String signingKeyName;
    
    private JWTInvalidationEvent(String signingKeyName) {
      this.signingKeyName = signingKeyName;
    }
  }
  
  private class InvalidationEventsHandler extends Thread {
    
    private void drain() {
      List<JWTInvalidationEvent> events = new ArrayList<>(invalidationEvents.size());
      invalidationEvents.drainTo(events);
      for (JWTInvalidationEvent event : events) {
        revokeInternal(event.signingKeyName);
      }
    }
    
    @Override
    public void run() {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          JWTInvalidationEvent event = invalidationEvents.take();
          revokeInternal(event.signingKeyName);
        } catch (InterruptedException ex) {
          LOG.info("JWT InvalidationEventHandler interrupted. Draining queue...");
          drain();
          Thread.currentThread().interrupt();
        }
      }
    }
  }
}
