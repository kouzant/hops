package io.hops.ha.common;

import io.hops.metadata.util.RMUtilities;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.NodeId;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by antonis on 3/23/16.
 */
public class AggregatedTransactionState extends TransactionStateImpl {

    private static final Log LOG = LogFactory.getLog(AggregatedTransactionState.class);

    private AtomicInteger pendingTransactionStates;

    public AggregatedTransactionState(TransactionType type,
            int pendingTransactionStates) {
        super(type);
        this.pendingTransactionStates = new AtomicInteger(pendingTransactionStates);
    }

    public AggregatedTransactionState(TransactionType type, int initialCounter,
            boolean batch, TransactionStateManager manager, int pendingTransactionStates) {
        super(type, initialCounter, batch, manager);
        this.pendingTransactionStates = new AtomicInteger(pendingTransactionStates);
    }

    @Override
    public void decCounter(Enum type) throws IOException {
        int counter = pendingTransactionStates.decrementAndGet();
        LOG.info("Counter is: " + counter);
        if (counter == 0) {
            LOG.info("Counter reached zero, commit!");
            // Commit the aggregated transaction state
            //RMUtilities.finishRPC(this);
            LOG.info("Printing toAddContainers");
            printMap(toAddContainers);
            LOG.info("Printing toUpdateContainers");
            printMap(toUpdateContainers);
            LOG.info("Printing toRemoveContainers");
            printMap(toRemoveContainers);
        }
    }

    public void aggregate(TransactionState ts) {
        if (ts instanceof TransactionStateImpl) {
            TransactionStateImpl tsImpl = (TransactionStateImpl) ts;

            aggregateContainersToAdd(tsImpl);
            aggregateContainersToUpdate(tsImpl);
            aggregateContainersToRemove(tsImpl);
            aggregateRMNodesToUpdate(tsImpl);
            aggregateRMNodeInfos(tsImpl);
            aggregateFiCaSchedulerNodeInfoToUpdate(tsImpl);
            aggregateFiCaSchedulerNodeInfoToAdd(tsImpl);
            aggregateFiCaSchedulerNodeInfoToRemove(tsImpl);
            aggregateRPCIds(tsImpl);
        } else {
            LOG.info("Transaction state " + ts.getId() + " is not of TransactionStateImpl" +
                    "and cannot aggregate!");
        }
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
        // Put toAggregate Sets into this sets
    }

    private void aggregateRPCIds(TransactionStateImpl ts) {
        for (Integer rpcId : ts.getRPCIds()) {
            addRPCId(rpcId);
        }
    }

    private <K, V> void genericMapAggregate(Map<K, V> source, Map<K, V> aggregated) {
        for (Map.Entry<K, V> entry : source.entrySet()) {
            aggregated.put(entry.getKey(), entry.getValue());
        }
    }

    private <T> void printMap(Map<String, T> map) {
        for (Map.Entry<String, T> entry : map.entrySet()) {
            LOG.info("Key: " + entry.getKey());
            LOG.info("Value: " + entry.getValue());
        }
    }
}
