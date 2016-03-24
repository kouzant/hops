package io.hops.ha.common;

import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.util.YarnAPIStorageFactory;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.NDBRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.*;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
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
}
