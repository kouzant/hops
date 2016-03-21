package io.hops.metadata.util;

import io.hops.common.GlobalThreadPool;
import io.hops.ha.common.TransactionState;
import io.hops.ha.common.TransactionStateImpl;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.*;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.junit.Before;
import org.junit.Test;
import sun.util.calendar.LocalGregorianCalendar;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by antonis on 3/18/16.
 */
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

        Object lock = new Object();

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
        ts.decCounter(TransactionState.TransactionType.APP);
        ts1.decCounter(TransactionState.TransactionType.APP);

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
