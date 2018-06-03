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
package org.apache.hadoop.util;

import java.util.Random;

public class ExponentialBackOff implements BackOff {
  private static final long INITIAL_INTERVAL_MILLIS = 100;
  private static final long MAXIMUM_INTERVAL_MILLIS = 60000;
  private static final double RANDOMIZATION_FACTOR = 0.5;
  private static final double MULTIPLIER = 1.5;
  private static final int MAXIMUM_RETRIES = 10;
  private static final Random RANDOM = new Random();
  
  private long initialIntervalMillis;
  private long maximumIntervalMillis;
  private double randomizationFactor;
  private double multiplier;
  private int maximumRetries;
  
  private int numberOfRetries;
  private long currentIntervalMillis;
  
  public ExponentialBackOff() {
    this(new Builder());
  }
  
  public ExponentialBackOff(Builder builder) {
    initialIntervalMillis = builder.initialIntervalMillis;
    maximumIntervalMillis = builder.maximumIntervalMillis;
    randomizationFactor = builder.randomizationFactor;
    multiplier = builder.multiplier;
    maximumRetries = builder.maximumRetries;
    reset();
  }
  
  @Override
  public long getBackOffInMillis() {
    if (++numberOfRetries > maximumRetries) {
      return -1;
    }
    
    double delta = randomizationFactor * currentIntervalMillis;
    double minInterval = currentIntervalMillis - delta;
    double maxInterval = currentIntervalMillis + delta;
    long interval = (long) (minInterval + (Math.random() * (maxInterval - minInterval + 1)));
    
    if (currentIntervalMillis >= maximumIntervalMillis / multiplier) {
      currentIntervalMillis = maximumIntervalMillis;
    } else {
      currentIntervalMillis *= multiplier;
    }
    
    return interval;
  }
  
  @Override
  public void reset() {
    currentIntervalMillis = initialIntervalMillis;
    numberOfRetries = 0;
  }
  
  public static class Builder {
    
    private long initialIntervalMillis = INITIAL_INTERVAL_MILLIS;
    private long maximumIntervalMillis = MAXIMUM_INTERVAL_MILLIS;
    private double randomizationFactor = RANDOMIZATION_FACTOR;
    private double multiplier = MULTIPLIER;
    private int maximumRetries = MAXIMUM_RETRIES;
    
    public ExponentialBackOff build() {
      return new ExponentialBackOff(this);
    }
    
    public Builder setInitialIntervalMillis(long initialIntervalMillis) {
      this.initialIntervalMillis = initialIntervalMillis;
      return this;
    }
    
    public Builder setMaximumIntervalMillis(long maximumIntervalMillis) {
      this.maximumIntervalMillis = maximumIntervalMillis;
      return this;
    }
    
    public Builder setRandomizationFactor(double randomizationFactor) {
      this.randomizationFactor = randomizationFactor;
      return this;
    }
    
    public Builder setMultiplier(double multiplier) {
      this.multiplier = multiplier;
      return this;
    }
    
    public Builder setMaximumRetries(int maximumRetries) {
      this.maximumRetries = maximumRetries;
      return this;
    }
  }
}
