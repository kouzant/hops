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

import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.yarn.dal.rmstatestore.AllocateRPCDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.GarbageCollectorRPCDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.HeartBeatRPCDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.RPCDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.appmasterrpc.*;
import io.hops.transaction.handler.LightWeightRequestHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RPCTestUtilities {

    // Creates records for table:
    // * yarn_appmaster_rpc
    public static RPC createAppMasterRPCs(int rpcId, byte[] payload) {

        return new RPC(rpcId, RPC.Type.Allocate, payload, "antonis");
    }

    // Creates records for tables:
    // * yarn_heartbeat_rpc
    // * yarn_heartbeat_container_statuses
    // * yarn_heartbeat_keepalive_app
    public static HeartBeatRPC createHeartBeatRPC(int rpcId, byte[] payload) {
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
    public static AllocateRPC createAllocateRPC(int rpcId, byte[] payload) {
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

    public static GarbageCollectorRPC createGarbageCollectorRPC(int rpcId, GarbageCollectorRPC.TYPE type) {
        return new GarbageCollectorRPC(rpcId, type);
    }

    // Helper classes
    public static class AddAppMasterRPC extends LightWeightRequestHandler {

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

    public static class RemoveAppMasterRPC extends LightWeightRequestHandler {

        private  List<RPC> toCommit;

        public RemoveAppMasterRPC() {
            super(YARNOperationType.TEST);
        }

        public void setToCommitSet(List<RPC> toCommit) {
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

    public static class AddHBRPC extends LightWeightRequestHandler {

        private HeartBeatRPC toCommit;

        public AddHBRPC() {
            super(YARNOperationType.TEST);
        }

        public void setToCommit(HeartBeatRPC toCommit) {
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

    public static class RemoveHBRPC extends LightWeightRequestHandler {

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

            hbRPCDAO.removeAll(hbRPC);

            long removeAllTime = System.currentTimeMillis();
            System.out.println("Time to removeAll (ms): " + (removeAllTime - lockTime));

            connector.commit();

            long commitTime = System.currentTimeMillis();
            System.out.println("Time to commit (ms): " + (commitTime - removeAllTime));
            return (System.currentTimeMillis() - startTime);
        }
    }

    public static class RemoveAllocRPC extends LightWeightRequestHandler {

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

            allocDAO.removeAll(allocRPCsToRemove);

            connector.commit();

            return (System.currentTimeMillis() - startTime);
        }
    }

    public static class AddAllocRPC extends LightWeightRequestHandler {

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

    public static class AddGarbageCollectorRPC extends LightWeightRequestHandler {

        private List<GarbageCollectorRPC> toCommit;

        public AddGarbageCollectorRPC() {
            super(YARNOperationType.TEST);
        }

        public void setToCommit(List<GarbageCollectorRPC> toCommit) {
            this.toCommit = toCommit;
        }

        @Override
        public Object performTask() throws IOException {
            long startTime = System.currentTimeMillis();
            connector.beginTransaction();
            connector.writeLock();
            GarbageCollectorRPCDataAccess gcDAO = (GarbageCollectorRPCDataAccess)
                    RMStorageFactory.getDataAccess(GarbageCollectorRPCDataAccess.class);

            gcDAO.addAll(toCommit);
            connector.commit();

            return (System.currentTimeMillis() - startTime);
        }
    }

    public static class GetHeartbeatRPCs extends LightWeightRequestHandler {

        public GetHeartbeatRPCs() {
            super(YARNOperationType.TEST);
        }

        @Override
        public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.readLock();
            HeartBeatRPCDataAccess hbDAO = (HeartBeatRPCDataAccess) RMStorageFactory
                    .getDataAccess(HeartBeatRPCDataAccess.class);
            Map<Integer, HeartBeatRPC> resultList = hbDAO.getAll();
            connector.commit();
            return resultList;
        }
    }

    public static class GetAllocateRPCs extends LightWeightRequestHandler {

        public GetAllocateRPCs() {
            super(YARNOperationType.TEST);
        }

        @Override
        public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.readLock();
            AllocateRPCDataAccess allocDAO = (AllocateRPCDataAccess) RMStorageFactory
                    .getDataAccess(AllocateRPCDataAccess.class);
            Map<Integer, AllocateRPC> resultList = allocDAO.getAll();
            connector.commit();

            return resultList;
        }
    }

    public static class GetGarbageCollectorRPCs extends LightWeightRequestHandler {

        public GetGarbageCollectorRPCs() {
            super(YARNOperationType.TEST);
        }

        @Override
        public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.readLock();
            GarbageCollectorRPCDataAccess gcDAO = (GarbageCollectorRPCDataAccess)
                    RMStorageFactory.getDataAccess(GarbageCollectorRPCDataAccess.class);
            List<GarbageCollectorRPC> resultList = gcDAO.getAll();
            connector.commit();

            return resultList;
        }
    }
}
