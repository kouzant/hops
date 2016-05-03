/*
 * Copyright 2016 Apache Software Foundation.
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

import io.hops.common.GlobalThreadPool;
import io.hops.exception.StorageException;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.yarn.dal.rmstatestore.AllocateRPCDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.GarbageCollectorRPCDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.HeartBeatRPCDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.appmasterrpc.GarbageCollectorRPC;
import io.hops.metadata.yarn.entity.appmasterrpc.RPC;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class GarbageCollectorService extends AbstractService {

    private final Log LOG = LogFactory.getLog(GarbageCollectorService.class);

    private final int RPCS_LIMIT = 1000;
    private final int RPCS_PER_THREAD = 30;
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
        super.serviceStop();
    }

    private class Worker implements Runnable {

        public Worker() {

        }

        @Override
        public void run() {
            try {
                LightWeightRequestHandler rpcsFetcher = new LightWeightRequestHandler(YARNOperationType.TEST) {
                    @Override
                    public Object performTask() throws IOException {
                        long start = System.currentTimeMillis();
                        connector.beginTransaction();
                        connector.readLock();

                        List<GarbageCollectorRPC> resultSet = gcDAO.getSubset(RPCS_LIMIT);

                        connector.commit();
                        LOG.info("Time to retrieve RPCs to collect: " + (System.currentTimeMillis() - start));
                        return resultSet;
                    }
                };

                while (!Thread.currentThread().isInterrupted()) {
                    List<GarbageCollectorRPC> rpcIdsToRemove = (List<GarbageCollectorRPC>) rpcsFetcher.handle();
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

                            if (head < rpcIdsToRemove.size()) {
                                List<GarbageCollectorRPC> lastSublist = rpcIdsToRemove
                                        .subList(head, rpcIdsToRemove.size());
                                collectors.add(new Remover(lastSublist));
                            }
                        }

                        LOG.debug("Going to create " + collectors.size() + " Removers");
                        long start = System.currentTimeMillis();
                        List<Future<Boolean>> futures = exec.invokeAll(collectors);
                        for (Future<Boolean> future : futures) {
                            future.get();
                        }

                        LOG.info("Time to remove " + rpcIdsToRemove.size() + " rubbishes: " + (System.currentTimeMillis() - start));
                    }
                }
            } catch (StorageException ex) {
                LOG.error(ex.getMessage(), ex);
                try {
                    TimeUnit.MILLISECONDS.sleep(500);
                } catch (InterruptedException ie) {
                    LOG.error(ex.getMessage(), ie);
                }
            } catch (InterruptedException ex) {
                LOG.debug("Stop interrupting me");
            } catch (ExecutionException ex) {
                LOG.error(ex.getMessage(), ex);
            } catch (IOException ex) {
                LOG.error(ex.getMessage(), ex);
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
                final List<RPC> hbRPCs =
                        new ArrayList<RPC>();
                final List<RPC> allocRPCs =
                        new ArrayList<RPC>();

                for (GarbageCollectorRPC rpc : rpcsToRemove) {
                    if (rpc.getType().equals(GarbageCollectorRPC.TYPE.HEARTBEAT)) {
                        hbRPCs.add(new RPC(rpc.getRpcid()));
                    } else {
                        allocRPCs.add(new RPC(rpc.getRpcid()));
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
                LOG.debug("Time to garbage collect (ms): " + commitTime);
                return true;
            } catch (StorageException ex) {
                LOG.error(ex.getMessage(), ex);
            } catch (IOException ex) {
                LOG.error(ex.getMessage(), ex);
            }

            return false;
        }
    }

}
