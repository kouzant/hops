package io.hops.ha.common;

import io.hops.metadata.yarn.entity.Container;
import io.hops.metadata.yarn.entity.SchedulerApplication;
import io.hops.metadata.yarn.entity.SchedulerApplicationInfoToAdd;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by antonis on 3/23/16.
 */
public class TestAggregateTxState {

    private static final Log LOG = LogFactory.getLog(TestAggregateTxState.class);

    @Test
    public void testContainersAggregation() throws Exception {
        TxWrapper ts0 = new TxWrapper(TransactionState.TransactionType.APP);
        TxWrapper ts1 = new TxWrapper(TransactionState.TransactionType.APP);
        TxWrapper ts2 = new TxWrapper(TransactionState.TransactionType.APP);

        List<TransactionStateImpl> txStates = new ArrayList<TransactionStateImpl>();
        txStates.add(ts0);
        txStates.add(ts1);
        txStates.add(ts2);

        // Containers to add
        Container cont0_0a = new Container("0_0a");
        Container cont0_1a = new Container("1_0a");
        Container cont2_0a = new Container("2_0a");

        ts0.toAddContainers.put(cont0_0a.getContainerId(), cont0_0a);
        ts0.toAddContainers.put(cont0_1a.getContainerId(), cont0_1a);
        ts2.toAddContainers.put(cont2_0a.getContainerId(), cont2_0a);

        // Containers to update
        Container cont0_0u = new Container("0_0u");
        Container cont1_0u = new Container("1_0u");
        Container cont1_1u = new Container("1_1u");

        ts0.toUpdateContainers.put(cont0_0u.getContainerId(), cont0_0u);
        ts1.toUpdateContainers.put(cont1_0u.getContainerId(), cont1_0u);
        ts1.toUpdateContainers.put(cont1_1u.getContainerId(), cont1_1u);

        // Containers to remove
        Container cont1_0r = new Container("1_0r");
        Container cont2_0r = new Container("2_0r");
        Container cont2_1r = new Container("2_1r");

        ts1.toRemoveContainers.put(cont1_0r.getContainerId(), cont1_0r);
        ts2.toRemoveContainers.put(cont2_0r.getContainerId(), cont2_0r);
        ts2.toRemoveContainers.put(cont2_1r.getContainerId(), cont2_1r);

        AggregatedTransactionState agrTx = new AggregatedTransactionState(
                TransactionState.TransactionType.APP);

        for (TransactionStateImpl tx : txStates) {
            agrTx.aggregate(tx);
            agrTx.decCounter(TransactionState.TransactionType.APP);
        }
    }

    @Test
    public void testSchedulerApplicationInfo() throws Exception {
        TxWrapper ts0 = new TxWrapper(TransactionState.TransactionType.APP);
        TxWrapper ts1 = new TxWrapper(TransactionState.TransactionType.APP);
        TxWrapper ts2 = new TxWrapper(TransactionState.TransactionType.APP);

        List<TxWrapper> tStates = new ArrayList<TxWrapper>();
        tStates.add(ts0);
        tStates.add(ts1);
        tStates.add(ts2);

        // SchedulerApplications to add
        ApplicationId appId0_0 = ApplicationId.newInstance(10L, 0);
        SchedulerApplicationInfoToAdd app0_0a = new SchedulerApplicationInfoToAdd(
                new SchedulerApplication(appId0_0.toString(), "antonis", "default"));
        ApplicationId appId0_1 = ApplicationId.newInstance(10L, 1);
        SchedulerApplicationInfoToAdd app0_1a = new SchedulerApplicationInfoToAdd(
                new SchedulerApplication(appId0_1.toString(), "antonis", "default"));
        ts0.schedulerApplicationInfo.setSchedulerApplicationsToAdd(appId0_0, app0_0a);
        ts0.schedulerApplicationInfo.setSchedulerApplicationsToAdd(appId0_1, app0_1a);

        ApplicationId appId1_0 = ApplicationId.newInstance(10L, 2);
        SchedulerApplicationInfoToAdd app1_0a = new SchedulerApplicationInfoToAdd(
                new SchedulerApplication(appId1_0.toString(), "antonis", "default"));
        ts1.schedulerApplicationInfo.setSchedulerApplicationsToAdd(appId1_0, app1_0a);

        AggregatedTransactionState argTx = new AggregatedTransactionState(TransactionState.TransactionType.APP);

        for (TxWrapper ts : tStates) {
            argTx.aggregate(ts);
            argTx.decCounter(TransactionState.TransactionType.APP);
        }

    }

    private class TxWrapper extends TransactionStateImpl {

        public TxWrapper(TransactionType type) {
            super(type);
        }
    }
}
