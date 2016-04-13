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
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSSchedulerNode;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class AggregatedTransactionState extends TransactionStateImpl {



    private static final Log LOG = LogFactory.getLog(AggregatedTransactionState.class);
    private final Set<TransactionState> aggregatedTs =
            new HashSet<TransactionState>();

    public AggregatedTransactionState(TransactionType type) {
        super(type);
    }

    public AggregatedTransactionState(TransactionType type, int initialCounter,
            boolean batch, TransactionStateManager manager) {
        super(type, initialCounter, batch, manager);
    }

    @Override
    public void commit(boolean flag) throws IOException {
        //LOG.info("Commit aggregated transaction state");
        //RMUtilities.logPutInCommitingQueue(this);
        //RMUtilities.finishRPC(this);
    }

    public void aggregate(TransactionState ts) {
        if (ts instanceof TransactionStateImpl) {
            aggregatedTs.add(ts);
            TransactionStateImpl tsImpl = (TransactionStateImpl) ts;
            //LOG.info("Aggregating TS: " + ts.getId());

            aggregateContainersToAdd(tsImpl);
            aggregateContainersToUpdate(tsImpl);
            aggregateContainersToRemove(tsImpl);
            aggregateRMNodesToUpdate(tsImpl);
            aggregateRMNodeInfos(tsImpl);
            aggregateFiCaSchedulerNodeInfoToUpdate(tsImpl);
            aggregateFiCaSchedulerNodeInfoToAdd(tsImpl);
            aggregateFiCaSchedulerNodeInfoToRemove(tsImpl);
            aggregateFairSchedulerNodeInfo(tsImpl);
            aggregateRMContainersToUpdate(tsImpl);
            aggregateRMContainersToRemove(tsImpl);
            aggregateCSQueueInfo(tsImpl);

            aggregateSchedulerApplicationInfo(tsImpl);
            aggregateApplicationsToAdd(tsImpl);
            aggregateUpdatedNodeIdToAdd(tsImpl);
            aggregateUpdatedNodeIdToRemove(tsImpl);
            aggregateApplicationsStateToRemove(tsImpl);
            aggregateAppAttempts(tsImpl);
            aggregateRanNodeToAdd(tsImpl);
            aggregateAllocateResponsesToAdd(tsImpl);
            aggregateAllocateResponsesToRemove(tsImpl);
            aggregateJustFinishedContainerToAdd(tsImpl);
            aggregateJustFinishedContainerToRemove(tsImpl);

            aggregateRMContextInfo(tsImpl);

            aggregatePersistedEventsToRemove(tsImpl);

            aggregateRPCIds(tsImpl);
            aggregateAppIds(tsImpl);
            aggregateNodeIds(tsImpl);
        } else {
            LOG.info("Transaction state " + ts.getId() + " is not of TransactionStateImpl" +
                    "and cannot aggregate!");
        }
    }

    public boolean hasAggregated() {
        return !appIds.isEmpty() || !nodesIds.isEmpty();
    }

    public Set<TransactionState> getAggregatedTs() {
        return aggregatedTs;
    }

    private void aggregateContainersToAdd(TransactionStateImpl ts) {
        genericMapAggregate(ts.toAddContainers, toAddContainers);
    }

    private void aggregateContainersToUpdate(TransactionStateImpl ts) {
        genericMapAggregate(ts.toUpdateContainers, toUpdateContainers);
    }

    private void aggregateContainersToRemove(TransactionStateImpl ts) {
        genericMapAggregate(ts.toRemoveContainers, toRemoveContainers);
    }

    private void aggregateRMNodesToUpdate(TransactionStateImpl ts) {
        genericMapAggregate(ts.rmNodesToUpdate, rmNodesToUpdate);
    }

    private void aggregateRMNodeInfos(TransactionStateImpl ts) {
        genericMapAggregate(ts.rmNodeInfos, rmNodeInfos);
    }

    private void aggregateFiCaSchedulerNodeInfoToUpdate(TransactionStateImpl ts) {
        genericMapAggregate(ts.ficaSchedulerNodeInfoToUpdate, ficaSchedulerNodeInfoToUpdate);
    }

    private void aggregateFiCaSchedulerNodeInfoToAdd(TransactionStateImpl ts) {
        genericMapAggregate(ts.ficaSchedulerNodeInfoToAdd, ficaSchedulerNodeInfoToAdd);
    }

    private void aggregateFiCaSchedulerNodeInfoToRemove(TransactionStateImpl ts) {
        genericMapAggregate(ts.ficaSchedulerNodeInfoToRemove, ficaSchedulerNodeInfoToRemove);
    }

    private void aggregateFairSchedulerNodeInfo(TransactionStateImpl ts) {
        FairSchedulerNodeInfo toAggregate = ts.getFairschedulerNodeInfo();
        Map<NodeId, FSSchedulerNode> fsSchedulerNodesToAdd =
                toAggregate.getFsSchedulerNodesToAdd();
        if (fsSchedulerNodesToAdd != null) {
            genericMapAggregate(fsSchedulerNodesToAdd,
                    fairschedulerNodeInfo.getFsSchedulerNodesToAdd());
        }

        Map<NodeId, FSSchedulerNode> fsSchedulerNodesToRemove =
                toAggregate.getFsSchedulerNodesToRemove();
        if (fsSchedulerNodesToRemove != null) {
            genericMapAggregate(fsSchedulerNodesToRemove,
                    fairschedulerNodeInfo.getFsSchedulerNodesToRemove());
        }
    }

    private void aggregateRMContainersToUpdate(TransactionStateImpl ts) {
        genericMapAggregate(ts.rmContainersToUpdate, rmContainersToUpdate);
    }

    private void aggregateRMContainersToRemove(TransactionStateImpl ts) {
        genericMapAggregate(ts.rmContainersToRemove, rmContainersToRemove);
    }

    private void aggregateCSQueueInfo(TransactionStateImpl ts) {
        genericMapAggregate(ts.csQueueInfo.getCSLeafQueueUserInfoToAdd(),
                csQueueInfo.getCSLeafQueueUserInfoToAdd());
        genericCollectionAggregate(ts.csQueueInfo.getUsersToRemove(),
                csQueueInfo.getUsersToRemove());
        genericMapAggregate(ts.csQueueInfo.getCSLeafQueuePendingAppToAdd(),
                csQueueInfo.getCSLeafQueuePendingAppToAdd());
        genericMapAggregate(ts.csQueueInfo.getCSLeafQueuePendingAppToRemove(),
                csQueueInfo.getCSLeafQueuePendingAppToRemove());
    }

    private void aggregateSchedulerApplicationInfo(TransactionStateImpl ts) {
        genericMapAggregate(ts.schedulerApplicationInfo.getSchedulerApplicationsToAdd(),
                schedulerApplicationInfo.getSchedulerApplicationsToAdd());

        //printMap(schedulerApplicationInfo.getSchedulerApplicationsToAdd());
        
        genericMapAggregate(ts.schedulerApplicationInfo.getFiCaSchedulerAppInfo(),
                schedulerApplicationInfo.getFiCaSchedulerAppInfo());
        genericCollectionAggregate(ts.schedulerApplicationInfo.getApplicationsIdToRemove(),
                schedulerApplicationInfo.getApplicationsIdToRemove());
    }

    private void aggregateApplicationsToAdd(TransactionStateImpl ts) {
        genericMapAggregate(ts.applicationsToAdd, applicationsToAdd);
    }

    private void aggregateUpdatedNodeIdToAdd(TransactionStateImpl ts) {
        genericMapAggregate(ts.updatedNodeIdToAdd, updatedNodeIdToAdd);
    }

    private void aggregateUpdatedNodeIdToRemove(TransactionStateImpl ts) {
        genericMapAggregate(ts.updatedNodeIdToRemove, updatedNodeIdToRemove);
    }

    private void aggregateApplicationsStateToRemove(TransactionStateImpl ts) {
        genericCollectionAggregate(ts.applicationsStateToRemove, applicationsStateToRemove);
    }

    private void aggregateAppAttempts(TransactionStateImpl ts) {
        genericMapAggregate(ts.appAttempts, appAttempts);
    }

    private void aggregateRanNodeToAdd(TransactionStateImpl ts) {
        genericMapAggregate(ts.ranNodeToAdd, ranNodeToAdd);
    }

    private void aggregateAllocateResponsesToAdd(TransactionStateImpl ts) {
        genericMapAggregate(ts.allocateResponsesToAdd, allocateResponsesToAdd);
    }

    private void aggregateAllocateResponsesToRemove(TransactionStateImpl ts) {
        genericMapAggregate(ts.allocateResponsesToRemove, allocateResponsesToRemove);
    }

    private void aggregateJustFinishedContainerToAdd(TransactionStateImpl ts) {
        genericMapAggregate(ts.justFinishedContainerToAdd, justFinishedContainerToAdd);
    }

    private void aggregateJustFinishedContainerToRemove(TransactionStateImpl ts) {
        genericMapAggregate(ts.justFinishedContainerToRemove, justFinishedContainerToRemove);
    }

    private void aggregateRMContextInfo(TransactionStateImpl ts) {
        genericMapAggregate(ts.rmcontextInfo.getActiveNodesToAdd(),
                rmcontextInfo.getActiveNodesToAdd());
        genericCollectionAggregate(ts.rmcontextInfo.getActiveNodesToRemove(),
                rmcontextInfo.getActiveNodesToRemove());
        genericCollectionAggregate(ts.rmcontextInfo.getInactiveNodesToAdd(),
                rmcontextInfo.getInactiveNodesToAdd());
        genericCollectionAggregate(ts.rmcontextInfo.getInactiveNodesToRemove(),
                rmcontextInfo.getInactiveNodesToRemove());
        rmcontextInfo.updateLoad(ts.rmcontextInfo.getRMHostname(),
                ts.rmcontextInfo.getLoad());
    }

    // Never used in TransactionStateImpl
    private void aggregatePersistedEventsToRemove(TransactionStateImpl ts) {
        genericCollectionAggregate(ts.persistedEventsToRemove,
                persistedEventsToRemove);
    }

    private void aggregateRPCIds(TransactionStateImpl ts) {
        for (Integer rpcId : ts.getRPCIds()) {
            addRPCId(rpcId);
        }
    }

    private void aggregateAppIds(TransactionStateImpl ts) {
        genericCollectionAggregate(ts.appIds, appIds);
    }

    private void aggregateNodeIds(TransactionStateImpl ts) {
        genericCollectionAggregate(ts.nodesIds, nodesIds);
    }

    private <T> void genericCollectionAggregate(Collection<T> source, Collection<T> target) {
        for (T val : source) {
            target.add(val);
        }
    }

    private <K, V> void genericMapAggregate(Map<K, V> source, Map<K, V> target) {
        for (Map.Entry<K, V> entry : source.entrySet()) {
            target.put(entry.getKey(), entry.getValue());
        }
    }

    private <K, V> void printMap(Map<K, V> map) {
        for (Map.Entry<K, V> entry : map.entrySet()) {
            LOG.info("Key: " + entry.getKey());
            LOG.info("Value: " + entry.getValue());
        }
    }
}
