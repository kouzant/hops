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
import io.hops.metadata.yarn.entity.appmasterrpc.GarbageCollectorRPC;
import io.hops.metadata.yarn.entity.appmasterrpc.RPC;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.service.Service;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TestGCService {

    private Configuration conf;
    final Random rand = new Random(1234L);
    final byte[] bigPayload = new byte[1024];
    final byte[] smallPayload = new byte[512];
    private final RPCTestUtilities.AddAppMasterRPC addAppMasterRPC =
            new RPCTestUtilities.AddAppMasterRPC();
    private final RPCTestUtilities.AddHBRPC addHBRPC =
            new RPCTestUtilities.AddHBRPC();
    private final RPCTestUtilities.AddAllocRPC addAllocRPC =
            new RPCTestUtilities.AddAllocRPC();
    private final RPCTestUtilities.AddGarbageCollectorRPC addGCRPC =
            new RPCTestUtilities.AddGarbageCollectorRPC();
    private final RPCTestUtilities.GetGarbageCollectorRPCs getGCRPC =
            new RPCTestUtilities.GetGarbageCollectorRPCs();
    private final RPCTestUtilities.GetHeartbeatRPCs getHBRPCs =
            new RPCTestUtilities.GetHeartbeatRPCs();
    private final RPCTestUtilities.GetAllocateRPCs getAllocRPCs =
            new RPCTestUtilities.GetAllocateRPCs();

    @Before
    public void setup() throws Exception {
        conf = new YarnConfiguration();
        YarnAPIStorageFactory.setConfiguration(conf);
        RMStorageFactory.setConfiguration(conf);
        rand.nextBytes(bigPayload);
        rand.nextBytes(smallPayload);
        RMUtilities.InitializeDB();
    }

    @Test
    public void testGC() throws Exception {
        persistRPCs(100);
        MockRM rm = new MockRM(conf);
        GarbageCollectorService gc = new GarbageCollectorService();
        gc.init(conf);
        rm.addService(gc);
        rm.start();

        Thread.sleep(5000);
        rm.stop();
        Thread.sleep(1000);

        List<GarbageCollectorRPC> gcRPCs = (List<GarbageCollectorRPC>) getGCRPC.handle();
        Assert.assertTrue(gcRPCs.isEmpty());
    }

    private void persistRPCs(int numOfRPCs) throws Exception {
        List<GarbageCollectorRPC> gcRPCs = new ArrayList<GarbageCollectorRPC>(numOfRPCs);

        for (int i = 0; i < numOfRPCs; ++i) {
            RPC appMaster = RPCTestUtilities.createAppMasterRPCs(i, bigPayload);
            addAppMasterRPC.setToCommit(appMaster);
            addAppMasterRPC.handle();

            if ((i % 2) == 0) {
                addHBRPC.setToCommit(RPCTestUtilities.createHeartBeatRPC(i, smallPayload));
                addHBRPC.handle();
                gcRPCs.add(RPCTestUtilities.createGarbageCollectorRPC(i, GarbageCollectorRPC.TYPE.HEARTBEAT));
            } else {
                addAllocRPC.setToCommit(RPCTestUtilities.createAllocateRPC(i, smallPayload));
                addAllocRPC.handle();
                gcRPCs.add(RPCTestUtilities.createGarbageCollectorRPC(i, GarbageCollectorRPC.TYPE.ALLOCATE));
            }
        }
        addGCRPC.setToCommit(gcRPCs);
        addGCRPC.handle();
    }
}
