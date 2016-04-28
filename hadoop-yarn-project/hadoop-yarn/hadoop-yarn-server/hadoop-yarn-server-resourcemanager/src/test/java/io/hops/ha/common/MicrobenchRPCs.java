package io.hops.ha.common;

import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.util.YarnAPIStorageFactory;
import io.hops.metadata.yarn.dal.rmstatestore.AllocateRPCDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.HeartBeatRPCDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.RPCDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.appmasterrpc.*;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Before;
import org.junit.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Created by antonis on 4/22/16.
 */
public class MicrobenchRPCs {

    private final Log LOG = LogFactory.getLog(MicrobenchRPCs.class);

    private Configuration conf;
    final int RUNS = 1;
    final int MIN_RPC = 100;
    final int MAX_RPC = 120;
    final int STEP_RPC = 100;
    final Random rand = new Random(1234L);
    final byte[] bigPayload = new byte[1024];
    final byte[] smallPayload = new byte[512];
    final AddAppMasterRPC addAppMasterRPC = new AddAppMasterRPC();
    final AddHBRPC addHBRPC = new AddHBRPC();
    final AddAllocRPC addAllocRPC = new AddAllocRPC();
    final RemoveAppMasterRPC removeAppMasterRPC = new RemoveAppMasterRPC();
    final List<RPC> rpcsToCommit = new ArrayList<RPC>(
            (int) Math.ceil(MAX_RPC/STEP_RPC));

    @Before
    public void setup() throws Exception {
        conf = new YarnConfiguration();
        YarnAPIStorageFactory.setConfiguration(conf);
        RMStorageFactory.setConfiguration(conf);
        RMUtilities.InitializeDB();
        rand.nextBytes(bigPayload);
        rand.nextBytes(smallPayload);
        rpcsToCommit.clear();
    }

    @Test
    public void benchAppMasterRPC() throws Exception {
        final int MIN_RPC = 100;
        final int MAX_RPC = 2000;
        final int STEP_RPC = 100;
        final int RUNS = 10;
        final Random rand = new Random(1234L);

        final List<RPC> toCommitSet = new ArrayList<RPC>(
                (int) Math.ceil(MAX_RPC/STEP_RPC));
        final AddAppMasterRPC addAppMasterRPC = new AddAppMasterRPC();
        final RemoveAppMasterRPC removeAppMasterRPC = new RemoveAppMasterRPC();

        final byte[] payload = new byte[1024];
        rand.nextBytes(payload);

        FileWriter writer;
        for (int r = 0 ; r < RUNS; ++r) {
            writer = new FileWriter("rpc_fk_bench/bench_all_fk_run-" + r, true);
            writer.write("NumOfRPCs,Persist time (ms),Remove time (ms)\n");

            for (int i = MIN_RPC; i <= MAX_RPC; i += STEP_RPC) {
                Long totalPersistTime = 0l;
                for (int j = 0; j < i; ++j) {
                    RPC rpc = createAppMasterRPCs(j, payload);
                    toCommitSet.add(rpc);

                    addAppMasterRPC.setToCommit(rpc);
                    Long persistTime = (Long) addAppMasterRPC.handle();
                    totalPersistTime += persistTime;
                }

                //LOG.info("Time to commit " + toCommitSet.size() + " AppMasterRPCs (ms): " + persistTime);

                removeAppMasterRPC.setToCommitSet(toCommitSet);
                Long removeTime = (Long) removeAppMasterRPC.handle();
                //LOG.info("Time to remove " + toCommitSet.size() + " AppMasterRPCs (ms): " + removeTime);
                writer.write(toCommitSet.size() + "," + totalPersistTime + "," + removeTime + "\n");

                toCommitSet.clear();
            }
            writer.flush();
            writer.close();
        }
    }

    @Test
    public void benchRPCsWithFK() throws Exception {
        FileWriter writer;

        for (int r = 0; r < RUNS; ++r) {
            writer = new FileWriter("rpc_fk_bench/bench_all_fk_run-" + r, true);
            writer.write("NumOfRPCs,Persist time (ms),Remove time (ms)\n");

            for (int i = MIN_RPC; i <= MAX_RPC; i += STEP_RPC) {
                RMUtilities.InitializeDB();
                Long totalPersistTime = 0L;

                for (int j = 0; j < i; ++j) {
                    RPC rpc = createAppMasterRPCs(j, bigPayload);
                    rpcsToCommit.add(rpc);
                    addAppMasterRPC.setToCommit(rpc);
                    Long appmasterPersist = (Long) addAppMasterRPC.handle();

                    addHBRPC.setToCommit(createHeartBeatRPC(j, smallPayload));
                    Long hbPersist = (Long) addHBRPC.handle();

                    addAllocRPC.setToCommit(createAllocateRPC(j, smallPayload));
                    Long allocPersist = (Long) addAllocRPC.handle();

                    totalPersistTime += (appmasterPersist + hbPersist + allocPersist);
                }

                removeAppMasterRPC.setToCommitSet(rpcsToCommit);
                Long removeTime = (Long) removeAppMasterRPC.handle();

                writer.write(rpcsToCommit.size() + "," + totalPersistTime + "," + removeTime + "\n");
                rpcsToCommit.clear();
                writer.flush();
            }
            writer.close();
        }
    }

    @Test
    public void benchRPCs() throws Exception {

        final RemoveHBRPC removeHBRPC = new RemoveHBRPC();
        final RemoveAllocRPC removeAllocRPC = new RemoveAllocRPC();

        final List<ToRemoveRPC> hbRPCsToRemove = new ArrayList<ToRemoveRPC>();
        final List<ToRemoveHBContainerStatus> hbContStatToRemove =
                new ArrayList<ToRemoveHBContainerStatus>();
        final List<ToRemoveHBKeepAliveApp> hbKeepAliveToRemove =
                new ArrayList<ToRemoveHBKeepAliveApp>();

        final List<ToRemoveRPC> allocRPCsToRemove = new ArrayList<ToRemoveRPC>();
        final List<ToRemoveAllocAsk> allocAsksToRemove = new ArrayList<ToRemoveAllocAsk>();
        final List<ToRemoveBlacklist> allocBlAddToRemove = new ArrayList<ToRemoveBlacklist>();
        final List<ToRemoveBlacklist> allocBlRemToRemove = new ArrayList<ToRemoveBlacklist>();
        final List<ToRemoveResource> allocReleaseToRemove = new ArrayList<ToRemoveResource>();
        final List<ToRemoveResource> allocIncreaseToRemove = new ArrayList<ToRemoveResource>();

        FileWriter writer;

        for (int r = 0; r < RUNS; ++r) {
            writer = new FileWriter("rpc_fk_bench/bench_no_fk_run-" + r, true);
            writer.write("NumOfRPCs,Persist time (ms),Remove time (ms)\n");

            for (int i = MIN_RPC; i <= MAX_RPC; i += STEP_RPC) {
                RMUtilities.InitializeDB();
                Long totalPersistTime = 0L;

                for (int j = 0; j < i; ++j) {
                    RPC rpc = createAppMasterRPCs(j, bigPayload);
                    rpcsToCommit.add(rpc);
                    addAppMasterRPC.setToCommit(rpc);
                    Long appmasterPersist = (Long) addAppMasterRPC.handle();

                    addHBRPC.setToCommit(createHeartBeatRPC(j, smallPayload));
                    Long hbPersist = (Long) addHBRPC.handle();

                    addAllocRPC.setToCommit(createAllocateRPC(j, smallPayload));
                    Long allocPersist = (Long) addAllocRPC.handle();

                    totalPersistTime += (appmasterPersist + hbPersist + allocPersist);
                }

                removeAppMasterRPC.setToCommitSet(rpcsToCommit);
                Long removeAppMasterTime = (Long) removeAppMasterRPC.handle();

                for (int j = 0; j < i; ++j) {
                    HeartBeatRPC hbRPC = createHeartBeatRPC(j, smallPayload);
                    // It should be equal to j
                    int rpcId = hbRPC.getRpcId();
                    hbRPCsToRemove.add(new ToRemoveRPC(rpcId));

                    for (String containerId : hbRPC.getContainersStatuses().keySet()) {
                        hbContStatToRemove.add(new ToRemoveHBContainerStatus(rpcId, containerId));
                    }

                    for (String appId : hbRPC.getKeepAliveApplications()) {
                        hbKeepAliveToRemove.add(new ToRemoveHBKeepAliveApp(rpcId, appId));
                    }

                    AllocateRPC allocRPC = createAllocateRPC(j, smallPayload);
                    rpcId = allocRPC.getRpcID();
                    allocRPCsToRemove.add(new ToRemoveRPC(rpcId));

                    for (String requestId : allocRPC.getAsk().keySet()) {
                        allocAsksToRemove.add(new ToRemoveAllocAsk(rpcId, requestId));
                    }

                    for (String resource : allocRPC.getBlackListAddition()) {
                        allocBlAddToRemove.add(new ToRemoveBlacklist(rpcId, resource));
                    }

                    for (String resource : allocRPC.getBlackListRemovals()) {
                        allocBlRemToRemove.add(new ToRemoveBlacklist(rpcId, resource));
                    }

                    for (String containerId : allocRPC.getReleaseList()) {
                        allocReleaseToRemove.add(new ToRemoveResource(rpcId, containerId));
                    }

                    for (String containerId : allocRPC.getResourceIncreaseRequest().keySet()) {
                        allocIncreaseToRemove.add(new ToRemoveResource(rpcId, containerId));
                    }
                }

                removeHBRPC.setToCommit(hbRPCsToRemove, hbContStatToRemove, hbKeepAliveToRemove);
                Long removeHBRPCTime = (Long) removeHBRPC.handle();

                removeAllocRPC.setCommit(allocRPCsToRemove, allocAsksToRemove, allocBlAddToRemove,
                        allocBlRemToRemove, allocReleaseToRemove, allocIncreaseToRemove);
                Long removeAllocRPCTime = (Long) removeAllocRPC.handle();

                System.out.println("RPCs: " + rpcsToCommit.size());
                System.out.println("removeAppMaster (ms): " + removeAppMasterTime);
                System.out.println("removeHBRPC (ms): " + removeHBRPCTime);
                System.out.println("removeAllocRPC (ms): " + removeAllocRPCTime);

                Long removeTime = removeAppMasterTime + removeHBRPCTime + removeAllocRPCTime;

                writer.write(rpcsToCommit.size() + "," + totalPersistTime + "," + removeTime + "\n");

                rpcsToCommit.clear();

                hbRPCsToRemove.clear();
                hbContStatToRemove.clear();
                hbKeepAliveToRemove.clear();

                allocRPCsToRemove.clear();
                allocAsksToRemove.clear();
                allocBlAddToRemove.clear();
                allocBlRemToRemove.clear();
                allocReleaseToRemove.clear();
                allocIncreaseToRemove.clear();
                writer.flush();
            }
            writer.close();
        }
    }

    // Creates records for table:
    // * yarn_appmaster_rpc
    private RPC createAppMasterRPCs(int rpcId, byte[] payload) {

        return new RPC(rpcId, RPC.Type.Allocate, payload, "antonis");
    }

    // Creates records for tables:
    // * yarn_heartbeat_rpc
    // * yarn_heartbeat_container_statuses
    // * yarn_heartbeat_keepalive_app
    private HeartBeatRPC createHeartBeatRPC(int rpcId, byte[] payload) {
        final int STATUSES = 50;
        final int KEEP_ALIVE = 50;

        Map<String, byte[]> containerStatuses = new HashMap<String, byte[]>(STATUSES);
        for (int i = 0; i < STATUSES; ++i) {
            containerStatuses.put("container_" + rpcId + "_" + i, payload);
        }
        List<String> keepAliveApplications = new ArrayList<String>(KEEP_ALIVE);
        for (int i = 0; i < KEEP_ALIVE; ++i) {
            keepAliveApplications.add("application_" + rpcId + "_" + i);
        }
        return new HeartBeatRPC("nodeId", 42, containerStatuses,
                keepAliveApplications, payload, payload, payload, rpcId);
    }

    // Creates record for tables:
    // * yarn_allocate_rpc
    // * yarn_allocate_rpc_ask
    // * yarn_allocate_rpc_blacklist_add
    // * yarn_allocate_rpc_blacklist_remove
    // * yarn_allocate_rpc_release
    // * yarn_allocate_rpc_resource_increase
    private AllocateRPC createAllocateRPC(int rpcId, byte[] payload) {
        final int RELEASE = 10;
        final int ASK = 10;
        final int INCREASE = 10;
        final int BL_ADD = 5;
        final int BL_REMOVE = 5;

        List<String> releaseSet = new ArrayList<String>(RELEASE);
        for (int i = 0; i < RELEASE; ++i) {
            releaseSet.add("container_" + rpcId + "_" + i);
        }

        Map<String, byte[]> askMap = new HashMap<String, byte[]>(ASK);
        for (int i = 0; i < ASK; ++i) {
            askMap.put("request_" + rpcId + "_" + i, payload);
        }

        Map<String, byte[]> increaseReq = new HashMap<String, byte[]>(INCREASE);
        for (int i = 0; i < INCREASE; ++i) {
            increaseReq.put("resource_" + rpcId + "_" + i, payload);
        }

        List<String> blacklistAdd = new ArrayList<String>(BL_ADD);
        for (int i = 0; i < BL_ADD; ++i) {
            blacklistAdd.add("resource_" + rpcId + "_" + i);
        }

        List<String> blacklistRemove = new ArrayList<String>(BL_REMOVE);
        for (int i = 0; i < BL_REMOVE; ++i) {
            blacklistRemove.add("resource_" + rpcId + "_" + i);
        }

        return new AllocateRPC(rpcId, 42, 0.4f, releaseSet, askMap, increaseReq, blacklistAdd, blacklistRemove);
    }

    // Helper classes
    private class AddAppMasterRPC extends LightWeightRequestHandler {

        private RPC toCommit;

        public AddAppMasterRPC() {
            super(YARNOperationType.TEST);
        }

        public void setToCommit(RPC toCommit) {
            this.toCommit = toCommit;
        }

        @Override
        public Object performTask() throws IOException {
            long startTime = System.currentTimeMillis();
            connector.beginTransaction();
            connector.writeLock();
            RPCDataAccess rpcDAO = (RPCDataAccess) RMStorageFactory
                    .getDataAccess(RPCDataAccess.class);

            rpcDAO.add(toCommit);

            connector.commit();
            return (System.currentTimeMillis() - startTime);
        }
    }

    private class RemoveAppMasterRPC extends LightWeightRequestHandler {

        private List<RPC> toCommit;

        public RemoveAppMasterRPC() {
            super(YARNOperationType.TEST);
        }

        private void setToCommitSet(List<RPC> toCommit) {
            this.toCommit = toCommit;
        }

        @Override
        public Object performTask() throws IOException {
            long startTime = System.currentTimeMillis();
            connector.beginTransaction();
            connector.writeLock();
            RPCDataAccess rpcDAO = (RPCDataAccess) RMStorageFactory
                    .getDataAccess(RPCDataAccess.class);
            rpcDAO.removeAll(toCommit);
            connector.commit();
            return (System.currentTimeMillis() - startTime);
        }
    }

    private class AddHBRPC extends LightWeightRequestHandler {

        private HeartBeatRPC toCommit;

        public AddHBRPC() {
            super(YARNOperationType.TEST);
        }

        private void setToCommit(HeartBeatRPC toCommit) {
            this.toCommit = toCommit;
        }

        @Override
        public Object performTask() throws IOException {
            long startTime = System.currentTimeMillis();
            connector.beginTransaction();
            connector.writeLock();
            HeartBeatRPCDataAccess hbRPCDAO = (HeartBeatRPCDataAccess) RMStorageFactory
                    .getDataAccess(HeartBeatRPCDataAccess.class);

            hbRPCDAO.add(toCommit);

            connector.commit();

            return (System.currentTimeMillis() - startTime);
        }
    }

    private class RemoveHBRPC extends LightWeightRequestHandler {

        private List<ToRemoveRPC> hbRPC;
        private List<ToRemoveHBContainerStatus> hbContStat;
        private List<ToRemoveHBKeepAliveApp> hbKeepAlive;

        public RemoveHBRPC() {
            super(YARNOperationType.TEST);
        }

        public void setToCommit(List<ToRemoveRPC> hbRPC, List<ToRemoveHBContainerStatus> hbContStat,
                List<ToRemoveHBKeepAliveApp> hbKeepAlive) {
            this.hbRPC = hbRPC;
            this.hbContStat = hbContStat;
            this.hbKeepAlive = hbKeepAlive;
        }

        @Override
        public Object performTask() throws IOException {
            long startTime = System.currentTimeMillis();
            connector.beginTransaction();
            connector.writeLock();
            long lockTime = System.currentTimeMillis();
            System.out.println("Time to take the lock (ms): " + (lockTime - startTime));
            HeartBeatRPCDataAccess hbRPCDAO = (HeartBeatRPCDataAccess) RMStorageFactory
                    .getDataAccess(HeartBeatRPCDataAccess.class);

            hbRPCDAO.removeAll(hbRPC, hbContStat, hbKeepAlive);

            long removeAllTime = System.currentTimeMillis();
            System.out.println("Time to removeAll (ms): " + (removeAllTime - lockTime));

            connector.commit();

            long commitTime = System.currentTimeMillis();
            System.out.println("Time to commit (ms): " + (commitTime - removeAllTime));
            return (System.currentTimeMillis() - startTime);
        }
    }

    private class RemoveAllocRPC extends LightWeightRequestHandler {

        private List<ToRemoveRPC> allocRPCsToRemove;
        private List<ToRemoveAllocAsk> allocAsksToRemove;
        private List<ToRemoveBlacklist> allocBlAddToRemove;
        private List<ToRemoveBlacklist> allocBlRemToRemove;
        private List<ToRemoveResource> allocReleaseToRemove;
        private List<ToRemoveResource> allocIncreaseToRemove;

        public RemoveAllocRPC() {
            super(YARNOperationType.TEST);
        }

        public void setCommit(List<ToRemoveRPC> allocRPCsToRemove, List<ToRemoveAllocAsk> allocAsksToRemove,
                List<ToRemoveBlacklist> allocBlAddToRemove, List<ToRemoveBlacklist> allocBlRemToRemove,
                List<ToRemoveResource> allocReleaseToRemove, List<ToRemoveResource> allocIncreaseToRemove) {
            this.allocRPCsToRemove = allocRPCsToRemove;
            this.allocAsksToRemove = allocAsksToRemove;
            this.allocBlAddToRemove = allocBlAddToRemove;
            this.allocBlRemToRemove = allocBlRemToRemove;
            this.allocReleaseToRemove = allocReleaseToRemove;
            this.allocIncreaseToRemove = allocIncreaseToRemove;
        }

        @Override
        public Object performTask() throws IOException {
            long startTime = System.currentTimeMillis();
            connector.beginTransaction();
            connector.writeLock();
            AllocateRPCDataAccess allocDAO = (AllocateRPCDataAccess) RMStorageFactory
                    .getDataAccess(AllocateRPCDataAccess.class);

            allocDAO.removeAll(allocRPCsToRemove, allocAsksToRemove, allocBlAddToRemove, allocBlRemToRemove,
                    allocReleaseToRemove, allocIncreaseToRemove);

            connector.commit();

            return (System.currentTimeMillis() - startTime);
        }
    }

    private class AddAllocRPC extends LightWeightRequestHandler {

        private AllocateRPC toCommit;

        public AddAllocRPC() {
            super(YARNOperationType.TEST);
        }

        public void setToCommit(AllocateRPC toCommit) {
            this.toCommit = toCommit;
        }

        @Override
        public Object performTask() throws IOException {
            long startTime = System.currentTimeMillis();
            connector.beginTransaction();
            connector.writeLock();
            AllocateRPCDataAccess allocRPCDAO = (AllocateRPCDataAccess) RMStorageFactory
                    .getDataAccess(AllocateRPCDataAccess.class);

            allocRPCDAO.add(toCommit);

            connector.commit();

            return (System.currentTimeMillis() - startTime);
        }
    }
}
