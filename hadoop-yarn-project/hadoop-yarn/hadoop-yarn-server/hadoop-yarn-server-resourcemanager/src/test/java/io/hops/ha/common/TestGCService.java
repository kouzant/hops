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
import io.hops.metadata.yarn.entity.rmstatestore.AllocateResponse;
import io.hops.metadata.yarn.entity.rmstatestore.GarbageCollectorAllocResp;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.NDBRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.fusesource.leveldbjni.All;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import sun.java2d.pipe.AlphaPaintPipe;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class TestGCService {

    private final Log LOG = LogFactory.getLog(TestGCService.class);

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
    private final AllocRespTestUtils.AllocateResponsesFetcher getAllocResp =
            new AllocRespTestUtils.AllocateResponsesFetcher();
    private final AllocRespTestUtils.AllocatedContainersFetcher getAllocCont =
            new AllocRespTestUtils.AllocatedContainersFetcher();
    private final AllocRespTestUtils.CompletedContStatusFetcher getComplCont =
            new AllocRespTestUtils.CompletedContStatusFetcher();
    private final AllocRespTestUtils.AllocRespGCFetcher getGCAllocResp =
            new AllocRespTestUtils.AllocRespGCFetcher();
    private final AllocRespTestUtils.ContainerAdder addContainers =
            new AllocRespTestUtils.ContainerAdder();

    private final int NUM_OF_ALLOC = 10;
    private final int NUM_OF_ALLOC_CONTAINERS = 10;
    private final int NUM_OF_CONTAINER_STATUSES = 10;

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
    public void testRPCGC() throws Exception {
        persistRPCs(113);
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

    @Test
    public void testAllocRespGC() throws Exception {
        MockRM rm = new MockRM(conf);
        GarbageCollectorService gc = new GarbageCollectorService();
        gc.init(conf);
        rm.addService(gc);
        rm.start();

        // Persist containers needed for RMState restore
        List<io.hops.metadata.yarn.entity.Container> containersToAdd =
                new ArrayList<io.hops.metadata.yarn.entity.Container>(NUM_OF_ALLOC_CONTAINERS);
        for (int i = 0; i < NUM_OF_ALLOC; ++i) {
            for (int j = 0; j < NUM_OF_ALLOC_CONTAINERS; ++j) {
                Container cont = AllocRespTestUtils.createContainer(AllocRespTestUtils.createApplicationAttemptId(i),
                        j);
                containersToAdd.add(new io.hops.metadata.yarn.entity.Container(cont.getId().toString(),
                        AllocRespTestUtils.getContainerBytes(cont)));
            }
        }
        addContainers.setToPersistSet(containersToAdd);
        addContainers.handle();

        TransactionStateImpl ts0 = new TransactionStateImpl(TransactionState.TransactionType.APP);
        AllocRespTestUtils.populateAllocateResponses(1, ts0, NUM_OF_ALLOC, NUM_OF_ALLOC_CONTAINERS,
                NUM_OF_CONTAINER_STATUSES);
        ts0.decCounter(TransactionState.TransactionType.APP);
        Thread.sleep(2000);

        Map<String, AllocateResponse> allocResponses = (Map<String, AllocateResponse>) getAllocResp.handle();
        Assert.assertEquals(NUM_OF_ALLOC, allocResponses.size());
        for (AllocateResponse resp : allocResponses.values()) {
            Assert.assertEquals(1, resp.getResponseId());
        }

        Map<String, List<AllocateResponse>> allocConts = (Map<String, List<AllocateResponse>>) getAllocCont.handle();
        Assert.assertEquals(NUM_OF_ALLOC, allocConts.size());

        Map<String, List<AllocateResponse>> complContStat = (Map<String, List<AllocateResponse>>) getComplCont.handle();
        Assert.assertEquals(NUM_OF_ALLOC, complContStat.size());


        TransactionStateImpl ts1 = new TransactionStateImpl(TransactionState.TransactionType.APP);
        AllocRespTestUtils.populateAllocateResponses(2, ts1, NUM_OF_ALLOC, NUM_OF_ALLOC_CONTAINERS,
                NUM_OF_CONTAINER_STATUSES);
        ts1.decCounter(TransactionState.TransactionType.APP);
        Thread.sleep(2000);

        allocResponses = (Map<String, AllocateResponse>) getAllocResp.handle();
        Assert.assertEquals(NUM_OF_ALLOC, allocResponses.size());
        for (AllocateResponse resp : allocResponses.values()) {
            Assert.assertEquals(2, resp.getResponseId());
        }

        allocConts = (Map<String, List<AllocateResponse>>) getAllocCont.handle();
        //AllocRespTestUtils.printAllocatedContainers(allocConts);

        complContStat = (Map<String, List<AllocateResponse>>) getComplCont.handle();
        //AllocRespTestUtils.printCompletedStatuses(complContStat);

        TransactionStateImpl ts3 = new TransactionStateImpl(TransactionState.TransactionType.APP);
        Map<ApplicationAttemptId, org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse> finalResponse =
                AllocRespTestUtils.populateAllocateResponses(3, ts3, NUM_OF_ALLOC - 1, NUM_OF_ALLOC_CONTAINERS,
                NUM_OF_CONTAINER_STATUSES);
        ts3.decCounter(TransactionState.TransactionType.APP);

        Thread.sleep(2000);

        allocResponses = (Map<String, AllocateResponse>) getAllocResp.handle();
        Assert.assertEquals(NUM_OF_ALLOC, allocResponses.size());
        // That's the application attempt that did not update the response ID previously
        String appAtt = AllocRespTestUtils.createApplicationAttemptId(NUM_OF_ALLOC - 1).toString();
        for (Map.Entry<String, AllocateResponse> resp : allocResponses.entrySet()) {
            if (resp.getKey().equals(appAtt)) {
                Assert.assertEquals(2, resp.getValue().getResponseId());
            } else {
                Assert.assertEquals(3, resp.getValue().getResponseId());
            }
        }

        List<GarbageCollectorAllocResp> gcAllocResp = (List<GarbageCollectorAllocResp>) getGCAllocResp.handle();
        Assert.assertEquals(0, gcAllocResp.size());

        NDBRMStateStore stateStore = new NDBRMStateStore();
        stateStore.init(conf);
        RMStateStore.RMState state = stateStore.loadState(rm.getRMContext());

        Map<ApplicationAttemptId, org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse> allocateResponseRec =
                state.getAllocateResponses();

        Assert.assertEquals(NUM_OF_ALLOC, allocateResponseRec.size());

        for (Map.Entry<ApplicationAttemptId, org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse> entry :
                finalResponse.entrySet()) {
            org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse resp = allocateResponseRec.get(entry.getKey());
            Assert.assertNotNull(resp);

            List<Container> allocatedConts = entry.getValue().getAllocatedContainers();
            List<Container> recoveredAllocConts = allocateResponseRec.get(entry.getKey()).getAllocatedContainers();
            Assert.assertNotNull(recoveredAllocConts);

            Assert.assertEquals(allocatedConts.size(), recoveredAllocConts.size());

            for (Container cont : allocatedConts) {
                boolean found = false;
                for (Container recoveredCont : recoveredAllocConts) {
                    if (cont.getId().toString().equals(recoveredCont.getId().toString())) {
                        found = true;
                        break;
                    }
                }
                Assert.assertTrue(found);
            }
        }
        rm.stop();
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
