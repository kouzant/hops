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
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.util.YarnAPIStorageFactory;
import io.hops.metadata.yarn.entity.appmasterrpc.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Before;
import org.junit.Test;

import java.io.FileWriter;
import java.util.*;

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
    final RPCTestUtilities.AddAppMasterRPC addAppMasterRPC = new RPCTestUtilities.AddAppMasterRPC();
    final RPCTestUtilities.AddHBRPC addHBRPC = new RPCTestUtilities.AddHBRPC();
    final RPCTestUtilities.AddAllocRPC addAllocRPC = new RPCTestUtilities.AddAllocRPC();
    final RPCTestUtilities.RemoveAppMasterRPC removeAppMasterRPC = new RPCTestUtilities.RemoveAppMasterRPC();
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
        final RPCTestUtilities.AddAppMasterRPC addAppMasterRPC = new RPCTestUtilities.AddAppMasterRPC();
        final RPCTestUtilities.RemoveAppMasterRPC removeAppMasterRPC = new RPCTestUtilities.RemoveAppMasterRPC();

        final byte[] payload = new byte[1024];
        rand.nextBytes(payload);

        FileWriter writer;
        for (int r = 0 ; r < RUNS; ++r) {
            writer = new FileWriter("rpc_fk_bench/bench_all_fk_run-" + r, true);
            writer.write("NumOfRPCs,Persist time (ms),Remove time (ms)\n");

            for (int i = MIN_RPC; i <= MAX_RPC; i += STEP_RPC) {
                Long totalPersistTime = 0l;
                for (int j = 0; j < i; ++j) {
                    RPC rpc = RPCTestUtilities.createAppMasterRPCs(j, payload);
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
                    RPC rpc = RPCTestUtilities.createAppMasterRPCs(j, bigPayload);
                    rpcsToCommit.add(rpc);
                    addAppMasterRPC.setToCommit(rpc);
                    Long appmasterPersist = (Long) addAppMasterRPC.handle();

                    addHBRPC.setToCommit(RPCTestUtilities.createHeartBeatRPC(j, smallPayload));
                    Long hbPersist = (Long) addHBRPC.handle();

                    addAllocRPC.setToCommit(RPCTestUtilities.createAllocateRPC(j, smallPayload));
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

        final RPCTestUtilities.RemoveHBRPC removeHBRPC = new RPCTestUtilities.RemoveHBRPC();
        final RPCTestUtilities.RemoveAllocRPC removeAllocRPC = new RPCTestUtilities.RemoveAllocRPC();

        final List<RPC> hbRPCsToRemove = new ArrayList<RPC>();

        final List<RPC> allocRPCsToRemove = new ArrayList<RPC>();

        FileWriter writer;

        for (int r = 0; r < RUNS; ++r) {
            writer = new FileWriter("rpc_fk_bench/bench_no_fk_run-" + r, true);
            writer.write("NumOfRPCs,Persist time (ms),Remove time (ms)\n");

            for (int i = MIN_RPC; i <= MAX_RPC; i += STEP_RPC) {
                RMUtilities.InitializeDB();
                Long totalPersistTime = 0L;

                for (int j = 0; j < i; ++j) {
                    RPC rpc = RPCTestUtilities.createAppMasterRPCs(j, bigPayload);
                    rpcsToCommit.add(rpc);
                    addAppMasterRPC.setToCommit(rpc);
                    Long appmasterPersist = (Long) addAppMasterRPC.handle();

                    addHBRPC.setToCommit(RPCTestUtilities.createHeartBeatRPC(j, smallPayload));
                    Long hbPersist = (Long) addHBRPC.handle();

                    addAllocRPC.setToCommit(RPCTestUtilities.createAllocateRPC(j, smallPayload));
                    Long allocPersist = (Long) addAllocRPC.handle();

                    totalPersistTime += (appmasterPersist + hbPersist + allocPersist);
                }

                removeAppMasterRPC.setToCommitSet(rpcsToCommit);
                Long removeAppMasterTime = (Long) removeAppMasterRPC.handle();

                for (int j = 0; j < i; ++j) {
                    HeartBeatRPC hbRPC = RPCTestUtilities.createHeartBeatRPC(j, smallPayload);
                    // It should be equal to j
                    int rpcId = hbRPC.getRpcId();
                    hbRPCsToRemove.add(new RPC(rpcId));

                    AllocateRPC allocRPC = RPCTestUtilities.createAllocateRPC(j, smallPayload);
                    rpcId = allocRPC.getRpcID();
                    allocRPCsToRemove.add(new RPC(rpcId));
                }

                removeHBRPC.setToCommit(hbRPCsToRemove);
                Long removeHBRPCTime = (Long) removeHBRPC.handle();

                removeAllocRPC.setCommit(allocRPCsToRemove);
                Long removeAllocRPCTime = (Long) removeAllocRPC.handle();

                System.out.println("RPCs: " + rpcsToCommit.size());
                System.out.println("removeAppMaster (ms): " + removeAppMasterTime);
                System.out.println("removeHBRPC (ms): " + removeHBRPCTime);
                System.out.println("removeAllocRPC (ms): " + removeAllocRPCTime);

                Long removeTime = removeAppMasterTime + removeHBRPCTime + removeAllocRPCTime;

                writer.write(rpcsToCommit.size() + "," + totalPersistTime + "," + removeTime + "\n");

                rpcsToCommit.clear();

                hbRPCsToRemove.clear();

                allocRPCsToRemove.clear();
                writer.flush();
            }
            writer.close();
        }
    }
}
