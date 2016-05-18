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
import io.hops.metadata.yarn.dal.rmstatestore.*;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.appmasterrpc.GarbageCollectorRPC;
import io.hops.metadata.yarn.entity.appmasterrpc.RPC;
import io.hops.metadata.yarn.entity.rmstatestore.AllocateResponse;
import io.hops.metadata.yarn.entity.rmstatestore.GarbageCollectorAllocResp;
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

    private final int RPCS_LIMIT = 400;
    private final int RPCS_PER_THREAD = 60;
    private final int ALLOC_RESP_PER_THREAD = 60;
    private Thread worker;
    private GarbageCollectorRPCDataAccess gcRPCDAO;
    private GarbageCollectorAllocRespDataAccess gcAllocRespDAO;
    private ExecutorService exec;

    public GarbageCollectorService() {
        super("GarbageCollectorService");
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        LOG.debug("Service init");
        gcRPCDAO = (GarbageCollectorRPCDataAccess) RMStorageFactory
                .getDataAccess(GarbageCollectorRPCDataAccess.class);
        gcAllocRespDAO = (GarbageCollectorAllocRespDataAccess) RMStorageFactory
                .getDataAccess(GarbageCollectorAllocRespDataAccess.class);
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

    private <T> List<Callable<Boolean>> createRemovalThreads(List<T> removables, int threadThreshold,
            Class<T> type) {
        List<Callable<Boolean>> callables = new ArrayList<Callable<Boolean>>();

        if (removables.size() <= threadThreshold) {
            addCallable(callables, removables, type);

            return callables;
        } else {
            int numOfThreads = (int) Math.ceil(removables.size() / threadThreshold);
            int head = 0;
            for (int i = 0; i < numOfThreads; ++i) {
                List<T> subList = removables.subList(head, head + threadThreshold);
                addCallable(callables, subList, type);
                head += threadThreshold;
            }

            if (head < removables.size()) {
                List<T> lastSubList = removables.subList(head, removables.size());
                addCallable(callables, lastSubList, type);
            }

            return callables;
        }
    }

    private <T> void addCallable(List<Callable<Boolean>> callables, List<T> subList, Class<T> type) {
        if (type.equals(GarbageCollectorRPC.class)) {
            callables.add(new RPCRemover((List<GarbageCollectorRPC>) subList));
        } else if (type.equals(GarbageCollectorAllocResp.class)) {
            callables.add(new AllocRespRemover((List<GarbageCollectorAllocResp>) subList));
        } else {
            LOG.error("Type is incompatible!");
        }
    }

    private class Worker implements Runnable {

        public Worker() {

        }

        @Override
        public void run() {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    RPCGarbageGetter rpcsFetcher = new RPCGarbageGetter();
                    AllocRespGarbageGetter allocRespFetcher = new AllocRespGarbageGetter();

                    List<GarbageCollectorRPC> rpcIdsToRemove = (List<GarbageCollectorRPC>) rpcsFetcher.handle();
                    List<GarbageCollectorAllocResp> allocRespToRemove = (List<GarbageCollectorAllocResp>)
                            allocRespFetcher.handle();

                    List<Callable<Boolean>> collectors =
                            new ArrayList<Callable<Boolean>>();

                    if (!rpcIdsToRemove.isEmpty()) {
                        collectors.addAll(createRemovalThreads(rpcIdsToRemove, RPCS_PER_THREAD,
                                GarbageCollectorRPC.class));
                    }

                    if (!allocRespToRemove.isEmpty()) {
                        collectors.addAll(createRemovalThreads(allocRespToRemove, ALLOC_RESP_PER_THREAD,
                                GarbageCollectorAllocResp.class));
                    }

                    List<Future<Boolean>> futures = exec.invokeAll(collectors);
                    for (Future<Boolean> future : futures) {
                        future.get();
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

    private class AllocRespRemover implements Callable<Boolean> {
        private final List<GarbageCollectorAllocResp> allocRespsToRemove;
        private final AllocatedContainersDataAccess allocContDAO = (AllocatedContainersDataAccess)
                RMStorageFactory.getDataAccess(AllocatedContainersDataAccess.class);
        private final CompletedContainersStatusDataAccess complContDAO = (CompletedContainersStatusDataAccess)
                RMStorageFactory.getDataAccess(CompletedContainersStatusDataAccess.class);
        private final GarbageCollectorAllocRespDataAccess gcAllocRespDAO = (GarbageCollectorAllocRespDataAccess)
                RMStorageFactory.getDataAccess(GarbageCollectorAllocRespDataAccess.class);

        public AllocRespRemover(List<GarbageCollectorAllocResp> allocRespsToRemove) {
            this.allocRespsToRemove = allocRespsToRemove;
        }

        @Override
        public Boolean call() throws Exception {

            try {
                final List<AllocateResponse> allocCont =
                        new ArrayList<AllocateResponse>();
                final List<AllocateResponse> complStatus =
                        new ArrayList<AllocateResponse>();

                for (GarbageCollectorAllocResp resp : allocRespsToRemove) {
                    if (resp.getType().equals(GarbageCollectorAllocResp.TYPE.ALLOCATED_CONTAINERS)) {
                        allocCont.add(new AllocateResponse(resp.getApplicationAttemptID(), resp.getResponseID()));
                    } else {
                        complStatus.add(new AllocateResponse(resp.getApplicationAttemptID(), resp.getResponseID()));
                    }
                }

                LightWeightRequestHandler allocRespHandler = new LightWeightRequestHandler(YARNOperationType.TEST) {
                    @Override
                    public Object performTask() throws IOException {
                        connector.beginTransaction();
                        connector.writeLock();

                        allocContDAO.removeGarbage(allocCont);
                        complContDAO.removeGarbage(complStatus);
                        gcAllocRespDAO.removeAll(allocRespsToRemove);

                        connector.commit();
                        return null;
                    }
                };

                allocRespHandler.handle();

                return true;
            } catch (StorageException ex) {
                LOG.error(ex.getMessage(), ex);
            }

            return false;
        }
    }

    private class RPCRemover implements Callable<Boolean> {

        private final List<GarbageCollectorRPC> rpcsToRemove;

        private final HeartBeatRPCDataAccess hbDAO = (HeartBeatRPCDataAccess) RMStorageFactory
                .getDataAccess(HeartBeatRPCDataAccess.class);
        private final AllocateRPCDataAccess allocDAO = (AllocateRPCDataAccess) RMStorageFactory
                .getDataAccess(AllocateRPCDataAccess.class);

        public RPCRemover(List<GarbageCollectorRPC> rpcsToRemove) {
            this.rpcsToRemove = rpcsToRemove;
        }

        @Override
        public Boolean call() {
            try {
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

                LightWeightRequestHandler rpcHandler = new LightWeightRequestHandler(YARNOperationType.TEST) {
                    @Override
                    public Object performTask() throws IOException {
                        connector.beginTransaction();
                        connector.writeLock();

                        hbDAO.removeGarbage(hbRPCs);
                        //connector.flush();
                        //LOG.debug("Removed Heartbeat GC");

                        allocDAO.removeGarbage(allocRPCs);
                        //connector.flush();
                        //LOG.debug("Removed Allocate GC");

                        gcRPCDAO.removeAll(rpcsToRemove);

                        connector.commit();
                        return null;
                    }
                };

                rpcHandler.handle();

                return true;
            } catch (StorageException ex) {
                LOG.error(ex.getMessage(), ex);
            } catch (IOException ex) {
                LOG.error(ex.getMessage(), ex);
            }

            return false;
        }
    }

    private class RPCGarbageGetter extends LightWeightRequestHandler {
        public RPCGarbageGetter() {
            super(YARNOperationType.TEST);
        }

        @Override
        public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.readLock();

            List<GarbageCollectorRPC> resultSet = gcRPCDAO.getSubset(RPCS_LIMIT);

            connector.commit();
            return resultSet;
        }
    }

    private class AllocRespGarbageGetter extends LightWeightRequestHandler {
        public AllocRespGarbageGetter() {
            super(YARNOperationType.TEST);
        }

        @Override
        public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.readCommitted();

            List<GarbageCollectorAllocResp> resultSet = gcAllocRespDAO
                    .getSubset(RPCS_LIMIT);

            connector.commit();
            return resultSet;
        }
    }
}
