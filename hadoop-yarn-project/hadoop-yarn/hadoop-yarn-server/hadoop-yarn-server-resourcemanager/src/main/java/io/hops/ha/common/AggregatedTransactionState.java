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
import io.hops.metadata.yarn.entity.appmasterrpc.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSSchedulerNode;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class AggregatedTransactionState extends TransactionStateImpl {

    private static final Log LOG = LogFactory.getLog(AggregatedTransactionState.class);

    public final boolean TESTING = false;

    private final Set<RMUtilities.ToBeAggregatedTS> aggregatedTs =
            new HashSet<RMUtilities.ToBeAggregatedTS>();

    // FOR EVALUATION
    public long commitRPCRemove = 0L;
    public long commitRMContextInfo = 0L;
    public long commitCSQueueInfo = 0L;
    public long commitRMNodeToUpdate = 0L;
    public long commitRMNodeInfo = 0L;
    public long commitTransactionState = 0L;
    public long commitFiCaSchedulerNodeInfo = 0L;
    public long commitFairSchedulerNodeInfo = 0L;
    public long commitSchedulerApplicationInfo = 0L;

    private final String containersToAdd = "ContainersToAdd";
    private final String containersToUpdate = "ContainersToUpdate";
    private final String containersToRemove = "ContainersToRemove";
    private final String RMNodesToUpdate = "RMNodesToUpdate";
    private final String RMNodeInfos = "RMNodeInfos";
    private final String FicaSchedulerNodeInfoToUpdate = "FicaSchedulerNodeInfoToUpdate";
    private final String FicaSchedulerNodeInfoToAdd = "FicaSchedulerNodeInfoToAdd";
    private final String FicaSchedulerNodeInfoToRemove = "FicaSchedulerNodeInfoToRemove";
    private final String FsSchedulerNodesToAdd = "FsSchedulerNodesToAdd";
    private final String FsSchedulerNodesToRemove = "FsSchedulerNodesToRemove";
    private final String RMContainersToUpdate = "RMContainersToUpdate";
    private final String RMContainersToRemove = "RMContainersToRemove";
    private final String CSLeafQueueUserInfoToAdd = "CSLeafQueueUserInfoToAdd";
    private final String CSQueueInfoUsersToRemove = "CSQueueInfoUsersToRemove";
    private final String CSLeafQueuePendingAppToAdd = "CSLeafQueuePendingAppToAdd";
    private final String CSLeafQueuePendingAppToRemove = "CSLeafQueuePendingAppToRemove";
    private final String SchedulerApplicationsToAdd = "SchedulerApplicationsToAdd";
    private final String FicaSchedulerAppInfo = "FicaSchedulerAppInfo";
    private final String ApplicationsIdToRemove = "ApplicationsIdToRemove";
    private final String ApplicationsToAdd = "ApplicationsToAdd";
    private final String UpdatedNodeIdToAdd = "UpdatedNodeIdToAdd";
    private final String UpdatedNodeIdToRemove = "UpdatedNodeIdToRemove";
    private final String ApplicationsStateToRemove = "ApplicationsStateToRemove";
    private final String AppAttempts = "AppAttempts";
    private final String RanNodeToAdd = "RanNodeToAdd";
    private final String AllocateResponsesToAdd = "AllocateResponsesToAdd";
    private final String AllocateResponsesToRemove = "AllocateResponsesToRemove";
    private final String JustFinishedContainerToAdd = "JustFinishedContainerToAdd";
    private final String JustFinishedContainerToRemove = "JustFinishedContainerToRemove";
    private final String ActiveNodesToAdd = "ActiveNodesToAdd";
    private final String ActiveNodesToRemove = "ActiveNodesToRemove";
    private final String InactiveNodesToAdd = "InactiveNodesToAdd";
    private final String InactiveNodesToRemove = "InactiveNodesToRemove";
    private final String PersistedEventsToRemove = "PersistedEventsToRemove";
    private final String NIJustLaunchedContainersToAdd = "NIJustLaunchedContainersToAdd";
    private final String NIJustLaunchedContainersToRemove = "NIJustLaunchedContainersToRemove";
    private final String NIContainersToCleanToAdd = "NIContainersToCleanToAdd";
    private final String NIContainersToCleanToRemove = "NIContainersToCleanToRemove";
    private final String NIFinishedApplicationToAdd = "NIFinishedApplicationToAdd";
    private final String NIFinishedApplicationToRemove = "NIFinishedApplicationToRemove";
    private final String NINodeUpdateQueueToAdd = "NINodeUpdateQueueToAdd";
    private final String NINodeUpdateQueueToRemove = "NINodeUpdateQueueToRemove";
    private final String NILatestHeartBeatResponseToAdd = "NILatestHeartBeatResponseToAdd";
    private final String NINextHeartBeat = "NINextHeartBeat";
    private final String NIPendingEventsToAdd = "NIPendingEventsToAdd";
    private final String NIPendingEventsToRemove = "NIPendingEventsToRemove";
    private final String RPCids = "RPCids";
    private final String AllocRPC = "allocRPC";
    private final String AllocRPCAsk = "allocRPCAsk";
    private final String AllocBlAdd = "allocBlAdd";
    private final String AllocBlRemove = "allocBlRemove";
    private final String AllocRelease = "allocRelease";
    private final String AllocIncrease = "allocIncrease";
    private final String HBRPCs = "hbRPCs";
    private final String HBContStat = "hbContStat";
    private final String HBKeepAlive = "hbKeepAlive";

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
        counters.put(AllocRPC, 0);
        counters.put(AllocRPCAsk, 0);
        counters.put(AllocBlAdd, 0);
        counters.put(AllocBlRemove, 0);
        counters.put(AllocRelease, 0);
        counters.put(AllocIncrease, 0);
        counters.put(HBRPCs, 0);
        counters.put(HBContStat, 0);
        counters.put(HBKeepAlive, 0);
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
            writer.write("commitRPCRemove,commitRMContextInfo,commitCSQueueInfo,commitRMNodeToUpdate,commitRMNodeInfo,"
                + "commitTransactionState,commitFiCaSchedulerNodeInfo,commitFairSchedulerNodeInfo,commitSchedulerApplicationInfo");
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

        sb.append(commitRPCRemove + "," + commitRMContextInfo + "," + commitCSQueueInfo + "," + commitRMNodeToUpdate
                + "," + commitRMNodeInfo + "," + commitTransactionState + "," + commitFiCaSchedulerNodeInfo
                + "," + commitFairSchedulerNodeInfo + "," + commitSchedulerApplicationInfo);
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
        if (TESTING) {
            initCounters();
        }
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

            aggregateAllocRPCToRemove(tsImpl);
            aggregateHeartbeartRPCsToRemove(tsImpl);

            // FOR TESTING with TestTSCommit#testTxOrdering3
            /*if (toAddContainers.size() > 3) {
                LOG.debug("Aggregated more than 3 RMContainersToAdd and I won't aggregate any more");
                return true;
            }*/

            if (rmContainersToUpdate.size() > 300) {
                return true;
            }

            if (allocateResponsesToAdd.size() > 100) {
                return true;
            }

            if (rmNodeInfos.size() > 150) {
                LOG.debug("Aggregated more than enough, have mercy on my soul!");
                return true;
            }

            if (schedulerApplicationInfo.getFiCaSchedulerAppInfo().size() > 100) {
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
        if (TESTING) {
            updateCounters(containersToAdd, ts.toAddContainers.size());
        }
        genericMapAggregate(ts.toAddContainers, toAddContainers);
    }

    private void aggregateContainersToUpdate(TransactionStateImpl ts) {
        if (TESTING) {
            updateCounters(containersToUpdate, ts.toUpdateContainers.size());
        }
        genericMapAggregate(ts.toUpdateContainers, toUpdateContainers);
    }

    private void aggregateContainersToRemove(TransactionStateImpl ts) {
        if (TESTING) {
            updateCounters(containersToRemove, ts.toRemoveContainers.size());
        }
        genericMapAggregate(ts.toRemoveContainers, toRemoveContainers);
    }

    private void aggregateRMNodesToUpdate(TransactionStateImpl ts) {
        if (TESTING) {
            updateCounters(RMNodesToUpdate, ts.rmNodesToUpdate.size());
        }
        genericMapAggregate(ts.rmNodesToUpdate, rmNodesToUpdate);
    }

    private void aggregateRMNodeInfos(TransactionStateImpl ts) {
        if (TESTING) {
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
        }
        //genericMapAggregate(ts.rmNodeInfos, rmNodeInfos);
        RMNodeInfo info = null;
        for (Map.Entry<NodeId, RMNodeInfo> entry : ts.rmNodeInfos.entrySet()) {
            if ((info = rmNodeInfos.get(entry.getKey())) == null) {
                rmNodeInfos.put(entry.getKey(), entry.getValue());
            } else {
                // Go through each element of entry.value and update the map
                RMNodeInfo value = entry.getValue();
                genericMapAggregate(value.persistedEventsToAdd, info.persistedEventsToAdd);
                genericCollectionAggregate(value.PendingEventsToRemove, info.PendingEventsToRemove);
                genericCollectionAggregate(value.containerToCleanToAdd, info.containerToCleanToAdd);
                genericCollectionAggregate(value.containerToCleanToRemove, info.containerToCleanToRemove);
                genericMapAggregate(value.justLaunchedContainersToAdd, info.justLaunchedContainersToAdd);
                genericMapAggregate(value.justLaunchedContainersToRemove, info.justLaunchedContainersToRemove);
                genericMapAggregate(value.nodeUpdateQueueToAdd, info.nodeUpdateQueueToAdd);
                genericMapAggregate(value.nodeUpdateQueueToRemove, info.nodeUpdateQueueToRemove);
                genericCollectionAggregate(value.finishedApplicationsToAdd, info.finishedApplicationsToAdd);
                genericCollectionAggregate(value.finishedApplicationsToRemove, info.finishedApplicationsToRemove);
                info.latestNodeHeartBeatResponse = value.latestNodeHeartBeatResponse;
                info.nextHeartbeat = value.nextHeartbeat;
                info.pendingId = value.pendingId;
            }
        }
    }

    private void aggregateFiCaSchedulerNodeInfoToUpdate(TransactionStateImpl ts) {
        if (TESTING) {
            updateCounters(FicaSchedulerNodeInfoToUpdate, ts.ficaSchedulerNodeInfoToUpdate.size());
        }
        //genericMapAggregate(ts.ficaSchedulerNodeInfoToUpdate, ficaSchedulerNodeInfoToUpdate);
        FiCaSchedulerNodeInfoToUpdate fica = null;
        for (Map.Entry<String, FiCaSchedulerNodeInfoToUpdate> entry : ts.ficaSchedulerNodeInfoToUpdate.entrySet()) {
            if ((fica = ficaSchedulerNodeInfoToUpdate.get(entry.getKey())) == null) {
                ficaSchedulerNodeInfoToUpdate.put(entry.getKey(), entry.getValue());
            } else {
                FiCaSchedulerNodeInfoToUpdate value = entry.getValue();
                fica.infoToUpdate = value.infoToUpdate;
                genericMapAggregate(value.launchedContainersToAdd, fica.launchedContainersToAdd);
                genericCollectionAggregate(value.launchedContainersToRemove, fica.launchedContainersToRemove);
                genericMapAggregate(value.toUpdateResources, fica.toUpdateResources);
                fica.id = value.id;

            }
        }
    }

    private void aggregateFiCaSchedulerNodeInfoToAdd(TransactionStateImpl ts) {
        if (TESTING) {
            updateCounters(FicaSchedulerNodeInfoToAdd, ts.ficaSchedulerNodeInfoToAdd.size());
        }
        genericMapAggregate(ts.ficaSchedulerNodeInfoToAdd, ficaSchedulerNodeInfoToAdd);
    }

    private void aggregateFiCaSchedulerNodeInfoToRemove(TransactionStateImpl ts) {
        if (TESTING) {
            updateCounters(FicaSchedulerNodeInfoToRemove, ts.ficaSchedulerNodeInfoToRemove.size());
        }
        genericMapAggregate(ts.ficaSchedulerNodeInfoToRemove, ficaSchedulerNodeInfoToRemove);
    }

    private void aggregateFairSchedulerNodeInfo(TransactionStateImpl ts) {
        FairSchedulerNodeInfo toAggregate = ts.getFairschedulerNodeInfo();
        Map<NodeId, FSSchedulerNode> fsSchedulerNodesToAdd =
                toAggregate.getFsSchedulerNodesToAdd();
        if (fsSchedulerNodesToAdd != null) {
            if (TESTING) {
                updateCounters(FsSchedulerNodesToAdd, fsSchedulerNodesToAdd.size());
            }
            genericMapAggregate(fsSchedulerNodesToAdd,
                    fairschedulerNodeInfo.getFsSchedulerNodesToAdd());
        }

        Map<NodeId, FSSchedulerNode> fsSchedulerNodesToRemove =
                toAggregate.getFsSchedulerNodesToRemove();
        if (fsSchedulerNodesToRemove != null) {
            if (TESTING) {
                updateCounters(FsSchedulerNodesToRemove, fsSchedulerNodesToRemove.size());
            }
            genericMapAggregate(fsSchedulerNodesToRemove,
                    fairschedulerNodeInfo.getFsSchedulerNodesToRemove());
        }
    }

    private void aggregateRMContainersToUpdate(TransactionStateImpl ts) {
        if (TESTING) {
            updateCounters(RMContainersToUpdate, ts.rmContainersToUpdate.size());
        }
        genericMapAggregate(ts.rmContainersToUpdate, rmContainersToUpdate);
    }

    private void aggregateRMContainersToRemove(TransactionStateImpl ts) {
        if (TESTING) {
            updateCounters(RMContainersToRemove, ts.rmContainersToRemove.size());
        }
        genericMapAggregate(ts.rmContainersToRemove, rmContainersToRemove);
    }

    private void aggregateCSQueueInfo(TransactionStateImpl ts) {
        if (TESTING) {
            updateCounters(CSLeafQueueUserInfoToAdd, ts.csQueueInfo.getCSLeafQueuePendingAppToAdd().size());
            updateCounters(CSQueueInfoUsersToRemove, ts.csQueueInfo.getUsersToRemove().size());
            updateCounters(CSLeafQueuePendingAppToAdd, ts.csQueueInfo.getCSLeafQueuePendingAppToAdd().size());
            updateCounters(CSLeafQueuePendingAppToRemove, ts.csQueueInfo.getCSLeafQueuePendingAppToRemove().size());
        }
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
        if (TESTING) {
            updateCounters(SchedulerApplicationsToAdd,
                    ts.schedulerApplicationInfo.getSchedulerApplicationsToAdd().size());
            updateCounters(FicaSchedulerAppInfo, ts.schedulerApplicationInfo.getFiCaSchedulerAppInfo().size());
            updateCounters(ApplicationsIdToRemove, ts.schedulerApplicationInfo.getApplicationsIdToRemove().size());
        }

        genericMapAggregate(ts.schedulerApplicationInfo.getSchedulerApplicationsToAdd(),
                schedulerApplicationInfo.getSchedulerApplicationsToAdd());

        //printMap(schedulerApplicationInfo.getSchedulerApplicationsToAdd());
        genericMapAggregate(ts.schedulerApplicationInfo.getFiCaSchedulerAppInfo(),
                schedulerApplicationInfo.getFiCaSchedulerAppInfo());

        genericCollectionAggregate(ts.schedulerApplicationInfo.getApplicationsIdToRemove(),
                schedulerApplicationInfo.getApplicationsIdToRemove());
    }

    private void aggregateApplicationsToAdd(TransactionStateImpl ts) {
        if (TESTING) {
            updateCounters(ApplicationsToAdd, ts.applicationsToAdd.size());
        }
        genericMapAggregate(ts.applicationsToAdd, applicationsToAdd);
    }

    private void aggregateUpdatedNodeIdToAdd(TransactionStateImpl ts) {
        if (TESTING) {
            updateCounters(UpdatedNodeIdToAdd, ts.updatedNodeIdToAdd.size());
        }
        genericMapAggregate(ts.updatedNodeIdToAdd, updatedNodeIdToAdd);
    }

    private void aggregateUpdatedNodeIdToRemove(TransactionStateImpl ts) {
        if (TESTING) {
            updateCounters(UpdatedNodeIdToRemove, ts.updatedNodeIdToRemove.size());
        }
        genericMapAggregate(ts.updatedNodeIdToRemove, updatedNodeIdToRemove);
    }

    private void aggregateApplicationsStateToRemove(TransactionStateImpl ts) {
        if (TESTING) {
            updateCounters(ApplicationsStateToRemove, ts.applicationsStateToRemove.size());
        }
        genericMapAggregate(ts.applicationsStateToRemove, applicationsStateToRemove);
    }

    private void aggregateAppAttempts(TransactionStateImpl ts) {
        if (TESTING) {
            updateCounters(AppAttempts, ts.appAttempts.size());
        }
        genericMapAggregate(ts.appAttempts, appAttempts);
    }

    private void aggregateRanNodeToAdd(TransactionStateImpl ts) {
        if (TESTING) {
            updateCounters(RanNodeToAdd, ts.ranNodeToAdd.size());
        }
        genericMapAggregate(ts.ranNodeToAdd, ranNodeToAdd);
    }

    private void aggregateAllocateResponsesToAdd(TransactionStateImpl ts) {
        if (TESTING) {
            updateCounters(AllocateResponsesToAdd, ts.allocateResponsesToAdd.size());
        }
        genericMapAggregate(ts.allocateResponsesToAdd, allocateResponsesToAdd);
    }

    private void aggregateAllocateResponsesToRemove(TransactionStateImpl ts) {
        if (TESTING) {
            updateCounters(AllocateResponsesToRemove, ts.allocateResponsesToRemove.size());
        }
        genericMapAggregate(ts.allocateResponsesToRemove, allocateResponsesToRemove);
    }

    private void aggregateJustFinishedContainerToAdd(TransactionStateImpl ts) {
        if (TESTING) {
            updateCounters(JustFinishedContainerToAdd, ts.justFinishedContainerToAdd.size());
        }
        genericMapAggregate(ts.justFinishedContainerToAdd, justFinishedContainerToAdd);
    }

    private void aggregateJustFinishedContainerToRemove(TransactionStateImpl ts) {
        if (TESTING) {
            updateCounters(JustFinishedContainerToRemove, ts.justFinishedContainerToRemove.size());
        }
        genericMapAggregate(ts.justFinishedContainerToRemove, justFinishedContainerToRemove);
    }

    private void aggregateRMContextInfo(TransactionStateImpl ts) {
        if (TESTING) {
            updateCounters(ActiveNodesToAdd, ts.rmcontextInfo.getActiveNodesToAdd().size());
            updateCounters(ActiveNodesToRemove, ts.rmcontextInfo.getActiveNodesToRemove().size());
            updateCounters(InactiveNodesToAdd, ts.rmcontextInfo.getInactiveNodesToAdd().size());
            updateCounters(InactiveNodesToRemove, ts.rmcontextInfo.getInactiveNodesToRemove().size());
        }

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
        if (TESTING) {
            updateCounters(PersistedEventsToRemove, ts.persistedEventsToRemove.size());
        }
        genericCollectionAggregate(ts.persistedEventsToRemove,
                persistedEventsToRemove);
    }

    private void aggregateRPCIds(TransactionStateImpl ts) {
        if (TESTING) {
            updateCounters(RPCids, ts.getRPCIds().size());
        }

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

    private void aggregateAllocRPCToRemove(TransactionStateImpl ts) {
        if (TESTING) {
            updateCounters(AllocRPC, ts.allocRPCToRemove.size());
            for (List<ToRemoveAllocAsk> item : ts.allocRPCAsk.values()) {
                updateCounters(AllocRPCAsk, item.size());
            }
            for (List<ToRemoveBlacklist> item : ts.allocBlAdd.values()) {
                updateCounters(AllocBlAdd, item.size());
            }
            for (List<ToRemoveBlacklist> item : ts.allocBlRemove.values()) {
                updateCounters(AllocBlRemove, item.size());
            }
            for (List<ToRemoveResource> item : ts.allocRelease.values()) {
                updateCounters(AllocRelease, item.size());
            }
            for (List<ToRemoveResource> item : ts.allocIncrease.values()) {
                updateCounters(AllocIncrease, item.size());
            }
        }

        genericMapAggregate(ts.allocRPCToRemove, allocRPCToRemove);
        genericMapAggregate(ts.allocRPCAsk, allocRPCAsk);
        genericMapAggregate(ts.allocBlAdd, allocBlAdd);
        genericMapAggregate(ts.allocBlRemove, allocBlRemove);
        genericMapAggregate(ts.allocRelease, allocRelease);
        genericMapAggregate(ts.allocIncrease, allocIncrease);
    }

    private void aggregateHeartbeartRPCsToRemove(TransactionStateImpl ts) {
        if (TESTING) {
            updateCounters(HBRPCs, ts.hbRPCToRemove.size());
            for (List<ToRemoveHBContainerStatus> item : ts.hbContStat.values()) {
                updateCounters(HBContStat, item.size());
            }
            for (List<ToRemoveHBKeepAliveApp> item : ts.hbKeepAlive.values()) {
                updateCounters(HBKeepAlive, item.size());
            }
        }

        genericMapAggregate(ts.hbRPCToRemove, hbRPCToRemove);
        genericMapAggregate(ts.hbContStat, hbContStat);
        genericMapAggregate(ts.hbKeepAlive, hbKeepAlive);
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
