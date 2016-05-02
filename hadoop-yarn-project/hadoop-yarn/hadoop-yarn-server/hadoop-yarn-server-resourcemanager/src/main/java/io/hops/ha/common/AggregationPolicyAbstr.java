/*
 * Copyright (C) 2016 hops.io.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.ha.common;

public abstract class AggregationPolicyAbstr implements AggregationPolicy {
    protected int aggregationLimit;
    protected boolean lastCommitStatus;

    public AggregationPolicyAbstr() {
        this(Integer.MAX_VALUE);
    }

    public AggregationPolicyAbstr(int initialLimit) {
        this.aggregationLimit = initialLimit;
        lastCommitStatus = false;
    }

    @Override
    public int getAggregationLimit() {
        return aggregationLimit;
    }

    @Override
    public boolean getLastCommitStatus() {
        return lastCommitStatus;
    }

    @Override
    public abstract void enforce(TransactionState ts);

    @Override
    public abstract void toggleSuccessfulCommitStatus();

    @Override
    public abstract void toggleFailedCommitStatus();
}
