package io.hops.ha.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;

import java.util.concurrent.TimeUnit;

/**
 * Created by antonis on 4/27/16.
 */
public class GarbageCollectorService extends AbstractService {

    private final Log LOG = LogFactory.getLog(GarbageCollectorService.class);
    private Thread worker;

    public GarbageCollectorService() {
        super("GarbageCollectorService");
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        LOG.debug("Service init");
    }

    @Override
    protected void serviceStart() throws Exception {
        LOG.info("Starting GarbageCollector service");
        worker = new Thread(new Worker());
        worker.setDaemon(true);
        worker.start();
        super.serviceStart();
    }

    @Override
    protected void serviceStop() throws Exception {
        LOG.debug("Stopping GarbageCollector service");
        worker.interrupt();
        LOG.info("GarbageCollector service stopped!");
    }

    private class Worker implements Runnable {

        public Worker() {

        }

        @Override
        public void run() {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    LOG.debug("I'm working");
                    TimeUnit.MILLISECONDS.sleep(100);
                }
            } catch (InterruptedException ex) {
                LOG.debug("Stop interrupting me");
            }
        }
    }
}
