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

import org.junit.Test;

public class TestExponentialBackOff {
  
  @Test
  public void testExponentialBackOff() {
    BackOff backOff = new ExponentialBackOff.Builder()
        .setInitialIntervalMillis(100)
        .setMaximumIntervalMillis(140)
        .setMaximumRetries(20)
        .build();
    for (int i = 0; i < 10; i++) {
      System.out.println("Backoff: " + backOff.getBackOffInMillis());
    }
  }
}
