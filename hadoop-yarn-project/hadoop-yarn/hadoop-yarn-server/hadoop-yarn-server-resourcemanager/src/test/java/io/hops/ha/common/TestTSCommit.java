package io.hops.ha.common;

import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.util.YarnAPIStorageFactory;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.NDBRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.*;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestTSCommit {
    private Configuration conf;

    @Before
    public void setup() throws Exception {
        conf = new YarnConfiguration();
        YarnAPIStorageFactory.setConfiguration(conf);
        RMStorageFactory.setConfiguration(conf);
        RMUtilities.InitializeDB();
    }

    @Test
    public void testApp() throws Exception {
        conf.setClass(YarnConfiguration.RM_SCHEDULER,
                FifoScheduler.class, ResourceScheduler.class);
        conf.setClass(YarnConfiguration.RM_STORE, NDBRMStateStore.class , RMStateStore.class);

        MockRM rm = new MockRM(conf);
        rm.start();

        MockNM nm0 = rm.registerNode("host0:1234", 10 * 1024, 6);
        MockNM nm1 = rm.registerNode("host1:1234", 10 * 1024, 6);

        List<MockNM> nms = new ArrayList<MockNM>(2);
        nms.add(nm0);
        nms.add(nm1);

        RMApp app0 = rm.submitApp(1024);
        RMApp app1 = rm.submitApp(1024);
        hbNodeManagers(nms);

        RMAppAttempt appAtt0 = app0.getCurrentAppAttempt();
        RMAppAttempt appAtt1 = app1.getCurrentAppAttempt();

        MockAM am0 = rm.sendAMLaunched(appAtt0.getAppAttemptId());
        am0.registerAppAttempt();

        MockAM am1 = rm.sendAMLaunched(appAtt1.getAppAttemptId());
        am1.registerAppAttempt();

        Thread.sleep(2000);

        System.out.println("Host for app0 master container: " + app0
                .getCurrentAppAttempt().getMasterContainer().getNodeId());
        System.out.println("Host for app1 master container: " + app1
                .getCurrentAppAttempt().getMasterContainer().getNodeId());

        // Allocate 3 containers for App0
        waitForAllocation(am0, nms, 3, "host0");

        // Allocate 4 containers for App1
        waitForAllocation(am1, nms, 4, "host1");

        Thread.sleep(2000);

        // Verify the resources allocated in the NodeManagers
        FifoScheduler scheduler = (FifoScheduler) rm.getResourceScheduler();
        SchedulerNodeReport nm0Report = scheduler.getNodeReport(nm0.getNodeId());
        SchedulerNodeReport nm1Report = scheduler.getNodeReport(nm1.getNodeId());

        System.out.print(printNodeUsage("NodeManager0", nm0Report));
        System.out.print(printNodeUsage("NodeManager1", nm1Report));

        // NodeManager0 should be full (+ master containers)
        Assert.assertEquals(10 * 1024, nm0Report.getUsedResource().getMemory());
        Assert.assertEquals(6, nm0Report.getUsedResource().getVirtualCores());

        // NodeManager1 should have the rest
        Assert.assertEquals(6 * 1024, nm1Report.getUsedResource().getMemory());
        Assert.assertEquals(3, nm1Report.getUsedResource().getVirtualCores());

        // Crash the first RM so that the second recover from DB
        //rm.stop();

        conf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
        NDBRMStateStore stateStore = new NDBRMStateStore();
        stateStore.init(conf);

        MockRM rm2 = new MockRM(conf, stateStore);
        rm2.start();

        FifoScheduler scheduler2 = (FifoScheduler) rm2.getResourceScheduler();

        SchedulerNodeReport nm0Report2 = scheduler2.getNodeReport(nm0.getNodeId());
        SchedulerNodeReport nm1Report2 = scheduler2.getNodeReport(nm1.getNodeId());

        System.out.println("Recovered ResourceManager");
        System.out.print(printNodeUsage("NodeManager0", nm0Report2));
        System.out.print(printNodeUsage("NodeManager1", nm1Report2));

        // NodeManager0 should be full (+ master containers)
        Assert.assertEquals(10 * 1024, nm0Report2.getUsedResource().getMemory());
        Assert.assertEquals(6, nm0Report2.getUsedResource().getVirtualCores());

        // NodeManager1 should have the rest
        Assert.assertEquals(6 * 1024, nm1Report2.getUsedResource().getMemory());
        Assert.assertEquals(3, nm1Report2.getUsedResource().getVirtualCores());

        rm2.stop();
    }

    private String printNodeUsage(String node, SchedulerNodeReport report) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append("Printing usage for node ").append(node).append("\n");
        sb.append("Number of containers: ").append(report.getNumContainers()).append("\n");
        sb.append("Memory used: ").append(report.getUsedResource().getMemory()).append("\n");
        sb.append("VCores used: ").append(report.getUsedResource().getVirtualCores()).append("\n");
        sb.append("\n");

        return sb.toString();
    }

    private void waitForAllocation(MockAM am, List<MockNM> nms,
            int numOfContainers, String host)
            throws Exception {
        am.allocate(host, 2 * 1024, numOfContainers, new ArrayList<ContainerId>());
        hbNodeManagers(nms);
        List<Container> conts = am.allocate(new ArrayList<ResourceRequest>(),
                new ArrayList<ContainerId>()).getAllocatedContainers();
        int contsReceived = conts.size();
        int waitCount = 0;
        while (contsReceived < numOfContainers && waitCount++ < 200) {
            Thread.sleep(100);
            conts = am.allocate(new ArrayList<ResourceRequest>(),
                    new ArrayList<ContainerId>()).getAllocatedContainers();
            contsReceived += conts.size();
            hbNodeManagers(nms);
        }
    }

    private void hbNodeManagers(List<MockNM> nms) throws Exception {
        for (MockNM nm : nms) {
            nm.nodeHeartbeat(true);
        }
    }

    @Test
    public void testTxOrdering2() throws Exception {
        MockRM rm = new MockRM(conf);

        ApplicationId appId0 = ApplicationId.newInstance(1L, 0);
        ApplicationAttemptId appAttId0 = ApplicationAttemptId.newInstance(appId0, 0);

        ApplicationId appId1 = ApplicationId.newInstance(2L, 1);
        ApplicationAttemptId appAttId1 = ApplicationAttemptId.newInstance(appId1, 1);

        // Add container for App0
        TransactionStateImpl ts0 = new TransactionStateImpl(TransactionState.TransactionType.APP);
        ts0.addRPCId(0);
        Container cont0_0 = Container.newInstance(
                ContainerId.newInstance(appAttId0, 100),
                NodeId.newInstance("host0", 1234),
                "127.0.0.1",
                Resource.newInstance(1024 * 6, 6),
                Priority.newInstance(1),
                null);

        RMContainer rmCont0_0 = new RMContainerImpl(cont0_0,
                appAttId0, NodeId.newInstance("host0", 1234),
                "antonis", rm.getRMContext(), ts0);

        ts0.addRMContainerToAdd((RMContainerImpl) rmCont0_0);
        ts0.decCounter(TransactionState.TransactionType.APP);

        // Add another container for App0
        TransactionStateImpl ts1 = new TransactionStateImpl(TransactionState.TransactionType.APP);
        ts1.addRPCId(1);
        Container cont0_1 = Container.newInstance(
                ContainerId.newInstance(appAttId0, 101),
                NodeId.newInstance("host0", 1234),
                "127.0.0.1",
                Resource.newInstance(1024 * 6, 6),
                Priority.newInstance(1),
                null);

        RMContainer rmCont0_1 = new RMContainerImpl(cont0_1,
                appAttId0, NodeId.newInstance("host0", 1234),
                "antonis", rm.getRMContext(), ts1);

        ts1.addRMContainerToAdd((RMContainerImpl) rmCont0_1);
        ts1.decCounter(TransactionState.TransactionType.APP);

        // Remove container cont0_0 for App0
        TransactionStateImpl ts2 = new TransactionStateImpl(TransactionState.TransactionType.APP);
        ts2.addRPCId(2);

        ts2.addRMContainerToRemove(rmCont0_0);
        ts2.decCounter(TransactionState.TransactionType.APP);

        // Add container for App1
        TransactionStateImpl ts3 = new TransactionStateImpl(TransactionState.TransactionType.APP);
        ts3.addRPCId(3);
        Container cont1_0 = Container.newInstance(
                ContainerId.newInstance(appAttId1, 200),
                NodeId.newInstance("host1", 1234),
                "127.0.0.1",
                Resource.newInstance(1024 * 6, 6),
                Priority.newInstance(1),
                null);

        RMContainer rmCont1_0 = new RMContainerImpl(cont1_0,
                appAttId1, NodeId.newInstance("host1", 1234),
                "antonis", rm.getRMContext(), ts3);

        ts3.addRMContainerToAdd((RMContainerImpl) rmCont1_0);
        ts3.decCounter(TransactionState.TransactionType.APP);

        // Remove containers for both App0 and App1
        TransactionStateImpl ts4 = new TransactionStateImpl(TransactionState.TransactionType.APP);
        ts4.addRPCId(4);
        ts4.addRMContainerToRemove(rmCont0_1);
        ts4.addRMContainerToRemove(rmCont1_0);
        ts4.decCounter(TransactionState.TransactionType.APP);

        // Add container for App0
        TransactionStateImpl ts5 = new TransactionStateImpl(TransactionState.TransactionType.APP);
        ts5.addRPCId(5);
        Container cont0_2 = Container.newInstance(
                ContainerId.newInstance(appAttId0, 102),
                NodeId.newInstance("host0", 1234),
                "127.0.0.1",
                Resource.newInstance(1024 * 6, 6),
                Priority.newInstance(1),
                null);

        RMContainer rmCont0_2 = new RMContainerImpl(cont0_2,
                appAttId0, NodeId.newInstance("host0", 1234),
                "antonis", rm.getRMContext(), ts5);

        ts5.addRMContainerToAdd((RMContainerImpl) rmCont0_2);
        ts5.decCounter(TransactionState.TransactionType.APP);

        Thread.sleep(3000);

        Map<String, io.hops.metadata.yarn.entity.RMContainer> result =
                RMUtilities.getAllRMContainers();

        Assert.assertEquals("There should be only one container persisted", 1,
                result.size());
        Assert.assertTrue("Container " + cont0_2.getId() + " should be there",
                result.containsKey(cont0_2.getId().toString()));

        rm.stop();
    }

    @Test
    public void testTxOrdering() throws Exception {
        MockRM rm = new MockRM(conf);

        ApplicationId appId0 = ApplicationId.newInstance(1L, 0);
        ApplicationAttemptId appAttId0 = ApplicationAttemptId.newInstance(appId0, 0);

        ApplicationId appId1 = ApplicationId.newInstance(2L, 1);
        ApplicationAttemptId appAttId1 = ApplicationAttemptId.newInstance(appId1, 1);

        TransactionStateImpl ts0 = new TsWrapper(TransactionState.TransactionType.APP);

        Container cont0 = Container.newInstance(
                ContainerId.newInstance(appAttId0, 100),
                NodeId.newInstance("host0", 1234),
                "127.0.0.1",
                Resource.newInstance(1024 * 6, 6),
                Priority.newInstance(1),
                null);

        RMContainer rmCont0 = new RMContainerImpl(cont0,
                appAttId0, NodeId.newInstance("host0", 1234),
                "antonis", rm.getRMContext(), ts0);

        Container cont1 = Container.newInstance(
                ContainerId.newInstance(appAttId0, 101),
                NodeId.newInstance("host0", 1234),
                "127.0.0.1",
                Resource.newInstance(1024 * 6, 6),
                Priority.newInstance(1),
                null);

        RMContainer rmCont1 = new RMContainerImpl(cont1,
                appAttId0, NodeId.newInstance("host0", 1234),
                "antonis", rm.getRMContext(), ts0);

        ts0.addRPCId(0);
        ts0.addRMContainerToAdd((RMContainerImpl) rmCont0);
        ts0.addRMContainerToAdd((RMContainerImpl) rmCont1);
        ts0.decCounter(TransactionState.TransactionType.APP);

        // Add container for another app
        TransactionStateImpl ts3 = new TransactionStateImpl(TransactionState.TransactionType.APP);
        ts3.addRPCId(3);
        Container cont1_0 = Container.newInstance(
                ContainerId.newInstance(appAttId1, 200),
                NodeId.newInstance("host1", 1234),
                "127.0.0.1",
                Resource.newInstance(1024 * 2, 2),
                Priority.newInstance(1),
                null);

        RMContainer rmCont1_0 = new RMContainerImpl(cont1_0,
                appAttId1, NodeId.newInstance("host1", 1234),
                "antonis", rm.getRMContext(), ts3);
        ts3.addRMContainerToAdd((RMContainerImpl) rmCont1_0);
        ts3.decCounter(TransactionState.TransactionType.APP);

        // Remove RMContainer
        TransactionStateImpl ts1 = new TransactionStateImpl(TransactionState.TransactionType.APP);
        ts1.addRPCId(1);
        ts1.addRMContainerToRemove(rmCont0);
        ts1.decCounter(TransactionState.TransactionType.APP);

        // Add a new container
        TransactionStateImpl ts2 = new TransactionStateImpl(TransactionState.TransactionType.APP);
        Container cont2 = Container.newInstance(
                ContainerId.newInstance(appAttId0, 102),
                NodeId.newInstance("host0", 1234),
                "127.0.0.1",
                Resource.newInstance(1024 * 6, 6),
                Priority.newInstance(1),
                null);

        RMContainer rmCont2 = new RMContainerImpl(cont2,
                appAttId0, NodeId.newInstance("host0", 1234),
                "antonis", rm.getRMContext(), ts2);

        ts2.addRPCId(2);
        ts2.addRMContainerToAdd((RMContainerImpl) rmCont2);
        ts2.decCounter(TransactionState.TransactionType.APP);

        Thread.sleep(10000);
        rm.stop();
    }

    @Test
    public void testQueueState() throws Exception {
        conf.setClass(YarnConfiguration.RM_SCHEDULER,
                FifoScheduler.class, ResourceScheduler.class);

        MockRM rm = new MockRM(conf);
        rm.start();

        MockNM nm = rm.registerNode("host0:1234", 6 * 1024, 6);

        RMApp app0 = rm.submitApp(1024, "name", "user", null, "queue1");
        RMApp app1 = rm.submitApp(1024, "name", "user", null, "queue1");

        nm.nodeHeartbeat(true);

        Thread.sleep(2000);

        RMAppAttempt appAtt0 = app0.getCurrentAppAttempt();
        RMAppAttempt appAtt1 = app1.getCurrentAppAttempt();

        MockAM am0 = rm.sendAMLaunched(appAtt0.getAppAttemptId());
        MockAM am1 = rm.sendAMLaunched(appAtt1.getAppAttemptId());

        TransactionState ts = new TransactionStateImpl(TransactionState.TransactionType.APP);
        ts.addRPCId(60);

        Thread blah = new Blah(app0, ts);
        blah.start();

        System.out.println("Continue our stuff");
        Assert.assertEquals(RMAppState.ACCEPTED, app0.getState());

        TransactionState ts1 = new TransactionStateImpl(TransactionState.TransactionType.APP);
        ts1.addRPCId(70);
        RMAppEvent appAtt0Failed = new RMAppFailedAttemptEvent(app0.getApplicationId(),
                RMAppEventType.ATTEMPT_FAILED, "Failed", true, ts1);
        app0.handle(appAtt0Failed);

        TransactionState ts2 = new TransactionStateImpl(TransactionState.TransactionType.APP);
        ts2.addRPCId(80);
        Assert.assertEquals(RMAppState.ACCEPTED, app1.getState());
        RMAppEvent appAtt1Failed = new RMAppFailedAttemptEvent(app1.getApplicationId(),
                RMAppEventType.ATTEMPT_FAILED, "Failed", true, ts2);
        app1.handle(appAtt1Failed);

        blah.join(4000);

        ts2.decCounter(TransactionState.TransactionType.APP);
        ts1.decCounter(TransactionState.TransactionType.APP);
        ts.decCounter(TransactionState.TransactionType.APP);

        Thread.sleep(2000);

        rm.stop();
    }

    private class Blah extends Thread {
        private final RMApp app;
        private final TransactionState ts;

        public Blah(RMApp app, TransactionState ts) {
            this.app = app;
            this.ts = ts;
        }

        @Override
        public void run() {

            try {
                //TransactionState ts = new TransactionStateImpl(TransactionState.TransactionType.APP);
                //ts.addRPCId(88);
                System.out.println("Sleeping TransactionState ID: " + ts.getId());
                System.out.println("Thread is sleeping");
                TimeUnit.SECONDS.sleep(3);
                System.out.println("Thread is awake!");
                RMAppEvent appAttFailed = new RMAppFailedAttemptEvent(app.getApplicationId(),
                        RMAppEventType.ATTEMPT_FAILED, "Failed", true, ts);
                app.handle(appAttFailed);
                //ts.decCounter(TransactionState.TransactionType.APP);
            /*} catch (IOException ex) {
                ex.printStackTrace();*/
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }
    }

    private class TsWrapper extends TransactionStateImpl {

        public TsWrapper(TransactionType type) {
            super(type);
        }
    }
}
