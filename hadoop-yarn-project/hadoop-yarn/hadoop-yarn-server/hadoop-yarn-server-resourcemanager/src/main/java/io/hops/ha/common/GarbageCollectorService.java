package io.hops.ha.common;

import io.hops.common.GlobalThreadPool;
import io.hops.exception.StorageException;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.yarn.dal.rmstatestore.AllocateRPCDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.GarbageCollectorRPCDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.HeartBeatRPCDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.appmasterrpc.GarbageCollectorRPC;
import io.hops.metadata.yarn.entity.appmasterrpc.ToRemoveRPC;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by antonis on 4/27/16.
 */
public class GarbageCollectorService extends AbstractService {

    private final Log LOG = LogFactory.getLog(GarbageCollectorService.class);

    private final int RPCS_LIMIT = 3000;
    private final int RPCS_PER_THREAD = 300;
    private Thread worker;
    private GarbageCollectorRPCDataAccess gcDAO;
    private ExecutorService exec;

    public GarbageCollectorService() {
        super("GarbageCollectorService");
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        LOG.debug("Service init");
        gcDAO = (GarbageCollectorRPCDataAccess) RMStorageFactory
                .getDataAccess(GarbageCollectorRPCDataAccess.class);
        exec = GlobalThreadPool.getExecutorService();
    }

    @Override
    protected void serviceStart() throws Exception {
        LOG.info("Starting GarbageCollector service");
        worker = new Thread(new Worker());
        worker.setName("GarbageCollector");
        worker.setDaemon(true);
        worker.start();
        super.serviceStart();
    }

    @Override
    protected void serviceStop() throws Exception {
        LOG.debug("Stopping GarbageCollector service");
        if (worker != null) {
            worker.interrupt();
        }
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
                    List<GarbageCollectorRPC> rpcIdsToRemove = gcDAO.getSubset(RPCS_LIMIT);
                    if (!rpcIdsToRemove.isEmpty()) {
                        LOG.debug("I will remove " + rpcIdsToRemove.size() + " RPCs");

                        List<Callable<Boolean>> collectors =
                                new ArrayList<Callable<Boolean>>();

                        if (rpcIdsToRemove.size() <= RPCS_PER_THREAD) {
                            collectors.add(new Remover(rpcIdsToRemove));
                        } else {
                            int numOfThreads = (int) Math.ceil(rpcIdsToRemove.size() / RPCS_PER_THREAD);
                            int head = 0;
                            for (int i = 0; i < numOfThreads; ++i) {
                                List<GarbageCollectorRPC> subList = rpcIdsToRemove
                                        .subList(head, head + RPCS_PER_THREAD);
                                collectors.add(new Remover(subList));
                                head += RPCS_PER_THREAD;
                            }

                            if (head < (rpcIdsToRemove.size() - 1)) {
                                List<GarbageCollectorRPC> lastSublist = rpcIdsToRemove
                                        .subList(head, rpcIdsToRemove.size());
                                collectors.add(new Remover(lastSublist));
                            }
                        }

                        LOG.debug("Going to create " + collectors.size() + " Removers");
                        exec.invokeAll(collectors);
                    }
                }
            } catch (StorageException ex) {
                LOG.debug(ex.getMessage());
            } catch (InterruptedException ex) {
                LOG.debug("Stop interrupting me");
            }
        }
    }

    private class Remover implements Callable<Boolean> {

        private final List<GarbageCollectorRPC> rpcsToRemove;
        private final HeartBeatRPCDataAccess hbDAO = (HeartBeatRPCDataAccess) RMStorageFactory
                .getDataAccess(HeartBeatRPCDataAccess.class);
        private final AllocateRPCDataAccess allocDAO = (AllocateRPCDataAccess) RMStorageFactory
                .getDataAccess(AllocateRPCDataAccess.class);

        public Remover(List<GarbageCollectorRPC> rpcsToRemove) {
            this.rpcsToRemove = rpcsToRemove;
        }


        @Override
        public Boolean call() {
            try {
                LOG.debug("I will remove " + rpcsToRemove.size() + " RPCs");
                final List<ToRemoveRPC> hbRPCs =
                        new ArrayList<ToRemoveRPC>();
                final List<ToRemoveRPC> allocRPCs =
                        new ArrayList<ToRemoveRPC>();

                for (GarbageCollectorRPC rpc : rpcsToRemove) {
                    if (rpc.getType().equals(GarbageCollectorRPC.TYPE.HEARTBEAT)) {
                        //LOG.debug("Adding RPC " + rpc.getRpcid() + " to HB garbage");
                        hbRPCs.add(new ToRemoveRPC(rpc.getRpcid()));
                    } else {
                        //LOG.debug("Adding RPC " + rpc.getRpcid() + " to Alloc garbage");
                        allocRPCs.add(new ToRemoveRPC(rpc.getRpcid()));
                    }
                }

                LightWeightRequestHandler dbHandler = new LightWeightRequestHandler(YARNOperationType.TEST) {
                    @Override
                    public Object performTask() throws IOException {
                        long startTime = System.currentTimeMillis();
                        connector.beginTransaction();
                        connector.writeLock();
                        hbDAO.removeGarbage(hbRPCs);
                        LOG.debug("Removed Heartbeat RPCs");
                        allocDAO.removeGarbage(allocRPCs);
                        LOG.debug("Removed Allocate RPCs");
                        gcDAO.removeAll(rpcsToRemove);
                        LOG.debug("Removed Garbage Collector RPC ids");

                        connector.commit();
                        return (System.currentTimeMillis() - startTime);
                    }
                };

                Long commitTime = (Long) dbHandler.handle();
                LOG.debug("Time to commit (ms): " + commitTime);
                return true;
            } catch (StorageException ex) {
                LOG.error(ex.getMessage());
            } catch (IOException ex) {
                LOG.error(ex.getMessage());
            }

            return false;
        }
    }

}
