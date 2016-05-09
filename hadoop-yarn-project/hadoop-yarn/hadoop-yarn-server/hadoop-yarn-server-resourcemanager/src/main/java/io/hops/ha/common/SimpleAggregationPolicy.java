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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SimpleAggregationPolicy extends AggregationPolicyAbstr {

    private static final Log LOG = LogFactory.getLog(SimpleAggregationPolicy.class);

    public SimpleAggregationPolicy() {
        super();
    }

    @Override
    public void enforce(TransactionState ts) {
        if (ts instanceof AggregatedTransactionState) {
            // Better be safe than sorry
            if (aggregationLimit < 2) {
                return;
            }
            LOG.info("aggregation limit was " + aggregationLimit);
            aggregationLimit = (int) Math.ceil(((AggregatedTransactionState) ts).getAggregatedTs().size() * 0.7);
            LOG.info("Reducing aggregation limit to " + aggregationLimit);
        }
    }

    @Override
    public void toggleSuccessfulCommitStatus() {
        LOG.debug("SimpleAggregationPolicy does not support commit status");
    }

    @Override
    public void toggleFailedCommitStatus() {
        LOG.debug("SimpleAggregationPolicy does not support commit status");
    }
}
