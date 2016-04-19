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

import io.hops.metadata.util.RMUtilities;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSSchedulerNode;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class AggregatedTransactionState extends TransactionStateImpl {

    private static final Log LOG = LogFactory.getLog(AggregatedTransactionState.class);

    public final boolean TESTING = true;

    private final Set<RMUtilities.ToBeAggregatedTS> aggregatedTs =
            new HashSet<RMUtilities.ToBeAggregatedTS>();

    // FOR EVALUATION
    private String containersToAdd = "ContainersToAdd";
    private String containersToUpdate = "ContainersToUpdate";
    private String containersToRemove = "ContainersToRemove";
    private String RMNodesToUpdate = "RMNodesToUpdate";
    private String RMNodeInfos = "RMNodeInfos";
    private String FicaSchedulerNodeInfoToUpdate = "FicaSchedulerNodeInfoToUpdate";
    private String FicaSchedulerNodeInfoToAdd = "FicaSchedulerNodeInfoToAdd";
    private String FicaSchedulerNodeInfoToRemove = "FicaSchedulerNodeInfoToRemove";
    private String FsSchedulerNodesToAdd = "FsSchedulerNodesToAdd";
    private String FsSchedulerNodesToRemove = "FsSchedulerNodesToRemove";
    private String RMContainersToUpdate = "RMContainersToUpdate";
    private String RMContainersToRemove = "RMContainersToRemove";
    private String CSLeafQueueUserInfoToAdd = "CSLeafQueueUserInfoToAdd";
    private String CSQueueInfoUsersToRemove = "CSQueueInfoUsersToRemove";
    private String CSLeafQueuePendingAppToAdd = "CSLeafQueuePendingAppToAdd";
    private String CSLeafQueuePendingAppToRemove = "CSLeafQueuePendingAppToRemove";
    private String SchedulerApplicationsToAdd = "SchedulerApplicationsToAdd";
    private String FicaSchedulerAppInfo = "FicaSchedulerAppInfo";
    private String ApplicationsIdToRemove = "ApplicationsIdToRemove";
    private String ApplicationsToAdd = "ApplicationsToAdd";
    private String UpdatedNodeIdToAdd = "UpdatedNodeIdToAdd";
    private String UpdatedNodeIdToRemove = "UpdatedNodeIdToRemove";
    private String ApplicationsStateToRemove = "ApplicationsStateToRemove";
    private String AppAttempts = "AppAttempts";
    private String RanNodeToAdd = "RanNodeToAdd";
    private String AllocateResponsesToAdd = "AllocateResponsesToAdd";
    private String AllocateResponsesToRemove = "AllocateResponsesToRemove";
    private String JustFinishedContainerToAdd = "JustFinishedContainerToAdd";
    private String JustFinishedContainerToRemove = "JustFinishedContainerToRemove";
    private String ActiveNodesToAdd = "ActiveNodesToAdd";
    private String ActiveNodesToRemove = "ActiveNodesToRemove";
    private String InactiveNodesToAdd = "InactiveNodesToAdd";
    private String InactiveNodesToRemove = "InactiveNodesToRemove";
    private String PersistedEventsToRemove = "PersistedEventsToRemove";
    private String NIJustLaunchedContainersToAdd = "NIJustLaunchedContainersToAdd";
    private String NIJustLaunchedContainersToRemove = "NIJustLaunchedContainersToRemove";
    private String NIContainersToCleanToAdd = "NIContainersToCleanToAdd";
    private String NIContainersToCleanToRemove = "NIContainersToCleanToRemove";
    private String NIFinishedApplicationToAdd = "NIFinishedApplicationToAdd";
    private String NIFinishedApplicationToRemove = "NIFinishedApplicationToRemove";
    private String NINodeUpdateQueueToAdd = "NINodeUpdateQueueToAdd";
    private String NINodeUpdateQueueToRemove = "NINodeUpdateQueueToRemove";
    private String NILatestHeartBeatResponseToAdd = "NILatestHeartBeatResponseToAdd";
    private String NINextHeartBeat = "NINextHeartBeat";
    private String NIPendingEventsToAdd = "NIPendingEventsToAdd";
    private String NIPendingEventsToRemove = "NIPendingEventsToRemove";
    private String RPCids = "RPCids";

    private final Map<String, Integer> counters =
            new HashMap<String, Integer>();

    private void initCounters() {
        counters.put(containersToAdd, 0);
        counters.put(containersToUpdate, 0);
        counters.put(containersToRemove, 0);
        counters.put(RMNodesToUpdate, 0);
        counters.put(RMNodeInfos, 0);
        counters.put(NIJustLaunchedContainersToAdd, 0);
        counters.put(NIJustLaunchedContainersToRemove, 0);
        counters.put(NIContainersToCleanToAdd, 0);
        counters.put(NIContainersToCleanToRemove, 0);
        counters.put(NIFinishedApplicationToAdd, 0);
        counters.put(NIFinishedApplicationToRemove, 0);
        counters.put(NINodeUpdateQueueToAdd, 0);
        counters.put(NINodeUpdateQueueToRemove, 0);
        counters.put(NILatestHeartBeatResponseToAdd, 0);
        counters.put(NINextHeartBeat, 0);
        counters.put(NIPendingEventsToAdd, 0);
        counters.put(NIPendingEventsToRemove, 0);
        counters.put(FicaSchedulerNodeInfoToUpdate, 0);
        counters.put(FicaSchedulerNodeInfoToAdd, 0);
        counters.put(FicaSchedulerNodeInfoToRemove, 0);
        counters.put(FsSchedulerNodesToAdd, 0);
        counters.put(FsSchedulerNodesToRemove, 0);
        counters.put(RMContainersToUpdate, 0);
        counters.put(RMContainersToRemove, 0);
        counters.put(CSLeafQueuePendingAppToAdd, 0);
        counters.put(CSLeafQueueUserInfoToAdd, 0);
        counters.put(CSQueueInfoUsersToRemove, 0);
        counters.put(CSLeafQueuePendingAppToRemove, 0);
        counters.put(SchedulerApplicationsToAdd, 0);
        counters.put(FicaSchedulerAppInfo, 0);
        counters.put(ApplicationsIdToRemove, 0);
        counters.put(ApplicationsToAdd, 0);
        counters.put(UpdatedNodeIdToAdd, 0);
        counters.put(UpdatedNodeIdToRemove, 0);
        counters.put(ApplicationsStateToRemove, 0);
        counters.put(AppAttempts, 0);
        counters.put(RanNodeToAdd, 0);
        counters.put(AllocateResponsesToAdd, 0);
        counters.put(AllocateResponsesToRemove, 0);
        counters.put(JustFinishedContainerToAdd, 0);
        counters.put(JustFinishedContainerToRemove, 0);
        counters.put(ActiveNodesToAdd, 0);
        counters.put(ActiveNodesToRemove, 0);
        counters.put(InactiveNodesToAdd, 0);
        counters.put(InactiveNodesToRemove, 0);
        counters.put(PersistedEventsToRemove, 0);
        counters.put(RPCids, 0);
    }

    private void updateCounters(String counterName, int size) {
        if (!TESTING) {
            return;
        }
        int newSize = counters.get(counterName) + size;
        counters.put(counterName, newSize);
    }

    public void printFileHeader(String fileName) {
        try {
            FileWriter writer = new FileWriter(fileName, true);
            for (String key : counters.keySet()) {
                writer.write(key + ",");
            }
            writer.write("\n");
            writer.flush();
            writer.close();
        } catch (IOException ex) {
            LOG.error(ex.getMessage());
        }
    }

    public void printCounters(String fileName) {
        StringBuilder sb = new StringBuilder();

        for (Integer value : counters.values()) {
            sb.append(value).append(",");
        }

        sb.append("\n");

        try {
            FileWriter writer = new FileWriter(fileName, true);
            writer.write(sb.toString());
            writer.flush();
            writer.close();
        } catch (IOException ex) {
            LOG.error("Could not write file " + ex.getMessage());
        }
    }

    public AggregatedTransactionState(TransactionType type, int initialCounter,
            boolean batch, TransactionStateManager manager) {
        super(type, initialCounter, batch, manager);
        initCounters();
    }

    @Override
    public void commit(boolean flag) throws IOException {
        //LOG.info("Commit aggregated transaction state");
        //RMUtilities.logPutInCommitingQueue(this);
        //RMUtilities.finishRPC(this);
    }

    public boolean aggregate(RMUtilities.ToBeAggregatedTS aggrTs) {
        if (aggrTs.getTs() instanceof TransactionStateImpl) {
            aggregatedTs.add(aggrTs);
            TransactionStateImpl tsImpl = (TransactionStateImpl) aggrTs.getTs();
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

            // FOR TESTING with TestTSCommit#testTxOrdering3
            /*if (toAddContainers.size() > 3) {
                LOG.debug("Aggregated more than 3 RMContainersToAdd and I won't aggregate any more");
                return true;
            }*/

            if (getRPCIds().size() > 100) {
                LOG.debug("Aggregated more than enough, have mercy on my soul!");
                return true;
            }
        } else {
            LOG.info("Transaction state " + aggrTs.getTs().getId() + " is not of TransactionStateImpl" +
                    "and cannot aggregate!");
        }
        return false;
    }

    public boolean hasAggregated() {
        return !appIds.isEmpty() || !nodesIds.isEmpty();
    }

    public Set<RMUtilities.ToBeAggregatedTS> getAggregatedTs() {
        return aggregatedTs;
    }

    private void aggregateContainersToAdd(TransactionStateImpl ts) {
        updateCounters(containersToAdd, ts.toAddContainers.size());
        genericMapAggregate(ts.toAddContainers, toAddContainers);
    }

    private void aggregateContainersToUpdate(TransactionStateImpl ts) {
        updateCounters(containersToUpdate, ts.toUpdateContainers.size());
        genericMapAggregate(ts.toUpdateContainers, toUpdateContainers);
    }

    private void aggregateContainersToRemove(TransactionStateImpl ts) {
        updateCounters(containersToRemove, ts.toRemoveContainers.size());
        genericMapAggregate(ts.toRemoveContainers, toRemoveContainers);
    }

    private void aggregateRMNodesToUpdate(TransactionStateImpl ts) {
        updateCounters(RMNodesToUpdate, ts.rmNodesToUpdate.size());
        genericMapAggregate(ts.rmNodesToUpdate, rmNodesToUpdate);
    }

    private void aggregateRMNodeInfos(TransactionStateImpl ts) {
        updateCounters(RMNodeInfos, ts.rmNodeInfos.size());

        for (RMNodeInfo info : ts.rmNodeInfos.values()) {
            updateCounters(NIJustLaunchedContainersToAdd, info.justLaunchedContainersToAdd.size());
            updateCounters(NIJustLaunchedContainersToRemove, info.justLaunchedContainersToRemove.size());
            updateCounters(NIContainersToCleanToAdd, info.containerToCleanToAdd.size());
            updateCounters(NIContainersToCleanToRemove, info.containerToCleanToRemove.size());
            updateCounters(NIFinishedApplicationToAdd, info.finishedApplicationsToAdd.size());
            updateCounters(NIFinishedApplicationToRemove, info.finishedApplicationsToRemove.size());
            updateCounters(NINodeUpdateQueueToAdd, info.nodeUpdateQueueToAdd.size());
            updateCounters(NINodeUpdateQueueToRemove, info.nodeUpdateQueueToRemove.size());
            updateCounters(NILatestHeartBeatResponseToAdd, 1);
            updateCounters(NINextHeartBeat, 1);
            updateCounters(NIPendingEventsToAdd, info.persistedEventsToAdd.size());
            updateCounters(NIPendingEventsToRemove, info.PendingEventsToRemove.size());
        }
        genericMapAggregate(ts.rmNodeInfos, rmNodeInfos);
    }

    private void aggregateFiCaSchedulerNodeInfoToUpdate(TransactionStateImpl ts) {
        updateCounters(FicaSchedulerNodeInfoToUpdate, ts.ficaSchedulerNodeInfoToUpdate.size());
        genericMapAggregate(ts.ficaSchedulerNodeInfoToUpdate, ficaSchedulerNodeInfoToUpdate);
    }

    private void aggregateFiCaSchedulerNodeInfoToAdd(TransactionStateImpl ts) {
        updateCounters(FicaSchedulerNodeInfoToAdd, ts.ficaSchedulerNodeInfoToAdd.size());
        genericMapAggregate(ts.ficaSchedulerNodeInfoToAdd, ficaSchedulerNodeInfoToAdd);
    }

    private void aggregateFiCaSchedulerNodeInfoToRemove(TransactionStateImpl ts) {
        updateCounters(FicaSchedulerNodeInfoToRemove, ts.ficaSchedulerNodeInfoToRemove.size());
        genericMapAggregate(ts.ficaSchedulerNodeInfoToRemove, ficaSchedulerNodeInfoToRemove);
    }

    private void aggregateFairSchedulerNodeInfo(TransactionStateImpl ts) {
        FairSchedulerNodeInfo toAggregate = ts.getFairschedulerNodeInfo();
        Map<NodeId, FSSchedulerNode> fsSchedulerNodesToAdd =
                toAggregate.getFsSchedulerNodesToAdd();
        if (fsSchedulerNodesToAdd != null) {
            updateCounters(FsSchedulerNodesToAdd, fsSchedulerNodesToAdd.size());
            genericMapAggregate(fsSchedulerNodesToAdd,
                    fairschedulerNodeInfo.getFsSchedulerNodesToAdd());
        }

        Map<NodeId, FSSchedulerNode> fsSchedulerNodesToRemove =
                toAggregate.getFsSchedulerNodesToRemove();
        if (fsSchedulerNodesToRemove != null) {
            updateCounters(FsSchedulerNodesToRemove, fsSchedulerNodesToRemove.size());
            genericMapAggregate(fsSchedulerNodesToRemove,
                    fairschedulerNodeInfo.getFsSchedulerNodesToRemove());
        }
    }

    private void aggregateRMContainersToUpdate(TransactionStateImpl ts) {
        updateCounters(RMContainersToUpdate, ts.rmContainersToUpdate.size());
        genericMapAggregate(ts.rmContainersToUpdate, rmContainersToUpdate);
    }

    private void aggregateRMContainersToRemove(TransactionStateImpl ts) {
        updateCounters(RMContainersToRemove, ts.rmContainersToRemove.size());
        genericMapAggregate(ts.rmContainersToRemove, rmContainersToRemove);
    }

    private void aggregateCSQueueInfo(TransactionStateImpl ts) {

        updateCounters(CSLeafQueueUserInfoToAdd, ts.csQueueInfo.getCSLeafQueuePendingAppToAdd().size());
        genericMapAggregate(ts.csQueueInfo.getCSLeafQueueUserInfoToAdd(),
                csQueueInfo.getCSLeafQueueUserInfoToAdd());

        updateCounters(CSQueueInfoUsersToRemove, ts.csQueueInfo.getUsersToRemove().size());
        genericCollectionAggregate(ts.csQueueInfo.getUsersToRemove(),
                csQueueInfo.getUsersToRemove());

        updateCounters(CSLeafQueuePendingAppToAdd, ts.csQueueInfo.getCSLeafQueuePendingAppToAdd().size());
        genericMapAggregate(ts.csQueueInfo.getCSLeafQueuePendingAppToAdd(),
                csQueueInfo.getCSLeafQueuePendingAppToAdd());

        updateCounters(CSLeafQueuePendingAppToRemove, ts.csQueueInfo.getCSLeafQueuePendingAppToRemove().size());
        genericMapAggregate(ts.csQueueInfo.getCSLeafQueuePendingAppToRemove(),
                csQueueInfo.getCSLeafQueuePendingAppToRemove());
    }

    private void aggregateSchedulerApplicationInfo(TransactionStateImpl ts) {
        updateCounters(SchedulerApplicationsToAdd, ts.schedulerApplicationInfo.getSchedulerApplicationsToAdd().size());
        genericMapAggregate(ts.schedulerApplicationInfo.getSchedulerApplicationsToAdd(),
                schedulerApplicationInfo.getSchedulerApplicationsToAdd());

        //printMap(schedulerApplicationInfo.getSchedulerApplicationsToAdd());

        updateCounters(FicaSchedulerAppInfo, ts.schedulerApplicationInfo.getFiCaSchedulerAppInfo().size());
        genericMapAggregate(ts.schedulerApplicationInfo.getFiCaSchedulerAppInfo(),
                schedulerApplicationInfo.getFiCaSchedulerAppInfo());

        updateCounters(ApplicationsIdToRemove, ts.schedulerApplicationInfo.getApplicationsIdToRemove().size());
        genericCollectionAggregate(ts.schedulerApplicationInfo.getApplicationsIdToRemove(),
                schedulerApplicationInfo.getApplicationsIdToRemove());
    }

    private void aggregateApplicationsToAdd(TransactionStateImpl ts) {
        updateCounters(ApplicationsToAdd, ts.applicationsToAdd.size());
        genericMapAggregate(ts.applicationsToAdd, applicationsToAdd);
    }

    private void aggregateUpdatedNodeIdToAdd(TransactionStateImpl ts) {
        updateCounters(UpdatedNodeIdToAdd, ts.updatedNodeIdToAdd.size());
        genericMapAggregate(ts.updatedNodeIdToAdd, updatedNodeIdToAdd);
    }

    private void aggregateUpdatedNodeIdToRemove(TransactionStateImpl ts) {
        updateCounters(UpdatedNodeIdToRemove, ts.updatedNodeIdToRemove.size());
        genericMapAggregate(ts.updatedNodeIdToRemove, updatedNodeIdToRemove);
    }

    private void aggregateApplicationsStateToRemove(TransactionStateImpl ts) {
        updateCounters(ApplicationsStateToRemove, ts.applicationsStateToRemove.size());
        genericCollectionAggregate(ts.applicationsStateToRemove, applicationsStateToRemove);
    }

    private void aggregateAppAttempts(TransactionStateImpl ts) {
        updateCounters(AppAttempts, ts.appAttempts.size());
        genericMapAggregate(ts.appAttempts, appAttempts);
    }

    private void aggregateRanNodeToAdd(TransactionStateImpl ts) {
        updateCounters(RanNodeToAdd, ts.ranNodeToAdd.size());
        genericMapAggregate(ts.ranNodeToAdd, ranNodeToAdd);
    }

    private void aggregateAllocateResponsesToAdd(TransactionStateImpl ts) {
        updateCounters(AllocateResponsesToAdd, ts.allocateResponsesToAdd.size());
        genericMapAggregate(ts.allocateResponsesToAdd, allocateResponsesToAdd);
    }

    private void aggregateAllocateResponsesToRemove(TransactionStateImpl ts) {
        updateCounters(AllocateResponsesToRemove, ts.allocateResponsesToRemove.size());
        genericMapAggregate(ts.allocateResponsesToRemove, allocateResponsesToRemove);
    }

    private void aggregateJustFinishedContainerToAdd(TransactionStateImpl ts) {
        updateCounters(JustFinishedContainerToAdd, ts.justFinishedContainerToAdd.size());
        genericMapAggregate(ts.justFinishedContainerToAdd, justFinishedContainerToAdd);
    }

    private void aggregateJustFinishedContainerToRemove(TransactionStateImpl ts) {
        updateCounters(JustFinishedContainerToRemove, ts.justFinishedContainerToRemove.size());
        genericMapAggregate(ts.justFinishedContainerToRemove, justFinishedContainerToRemove);
    }

    private void aggregateRMContextInfo(TransactionStateImpl ts) {
        updateCounters(ActiveNodesToAdd, ts.rmcontextInfo.getActiveNodesToAdd().size());
        genericMapAggregate(ts.rmcontextInfo.getActiveNodesToAdd(),
                rmcontextInfo.getActiveNodesToAdd());

        updateCounters(ActiveNodesToRemove, ts.rmcontextInfo.getActiveNodesToRemove().size());
        genericCollectionAggregate(ts.rmcontextInfo.getActiveNodesToRemove(),
                rmcontextInfo.getActiveNodesToRemove());

        updateCounters(InactiveNodesToAdd, ts.rmcontextInfo.getInactiveNodesToAdd().size());
        genericCollectionAggregate(ts.rmcontextInfo.getInactiveNodesToAdd(),
                rmcontextInfo.getInactiveNodesToAdd());

        updateCounters(InactiveNodesToRemove, ts.rmcontextInfo.getInactiveNodesToRemove().size());
        genericCollectionAggregate(ts.rmcontextInfo.getInactiveNodesToRemove(),
                rmcontextInfo.getInactiveNodesToRemove());

        rmcontextInfo.updateLoad(ts.rmcontextInfo.getRMHostname(),
                ts.rmcontextInfo.getLoad());
    }

    // Never used in TransactionStateImpl
    private void aggregatePersistedEventsToRemove(TransactionStateImpl ts) {
        updateCounters(PersistedEventsToRemove, ts.persistedEventsToRemove.size());
        genericCollectionAggregate(ts.persistedEventsToRemove,
                persistedEventsToRemove);
    }

    private void aggregateRPCIds(TransactionStateImpl ts) {
        updateCounters(RPCids, ts.getRPCIds().size());

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
