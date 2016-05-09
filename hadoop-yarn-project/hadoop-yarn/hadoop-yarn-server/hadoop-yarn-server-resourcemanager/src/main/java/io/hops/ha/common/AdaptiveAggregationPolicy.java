/*
 * Copyright 2016 Apache Software Foundation.
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

import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

public class AdaptiveAggregationPolicy extends AggregationPolicyAbstr {

    // FOR TESTING
    private final Queue<Integer> limits =
            new LinkedList<Integer>();

    private final Log LOG = LogFactory.getLog(AdaptiveAggregationPolicy.class);

    private final float DECREMENT_FACTOR = 0.7f;
    private final float INCREMENT_FACTOR = 0.2f;

    public AdaptiveAggregationPolicy() {
        super();
    }

    @Override
    public void enforce(TransactionState ts) {
        if (ts instanceof AggregatedTransactionState) {
            if (!lastCommitStatus) {
                // Last commit failed, decrease aggregation limit
                if (aggregationLimit < 2) {
                    return;
                }
                aggregationLimit = (int) Math.ceil(((AggregatedTransactionState) ts).
                        getAggregatedTs().size() * DECREMENT_FACTOR);
                LOG.info("Reducing aggregation limit to " + aggregationLimit);
                limits.add(aggregationLimit);
            } else {
                // Last commit succeed, increase aggregation limit
                aggregationLimit += Math.floor(aggregationLimit * INCREMENT_FACTOR);
                LOG.info("Incrementing aggregation limit to " + aggregationLimit);
                limits.add(aggregationLimit);
            }
        }
    }

    // FOR TESTING
    public void exportLimits() {
        try {
            FileWriter writer = new FileWriter("adaptive_limits", true);

            Integer head = null;

            while((head = limits.poll()) != null) {
                writer.write(head + ",");
            }
            writer.write("\n");
            writer.flush();
            writer.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }

    }
    @Override
    public void toggleSuccessfulCommitStatus() {
        lastCommitStatus = true;
    }

    @Override
    public void toggleFailedCommitStatus() {
        lastCommitStatus = false;
    }
}
