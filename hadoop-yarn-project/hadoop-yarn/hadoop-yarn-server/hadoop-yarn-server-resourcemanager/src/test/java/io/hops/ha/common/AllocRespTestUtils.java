package io.hops.ha.common;

import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.yarn.dal.ContainerDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.AllocateResponseDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.AllocatedContainersDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.CompletedContainersStatusDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.GarbageCollectorAllocRespDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.*;
import io.hops.metadata.yarn.entity.rmstatestore.GarbageCollectorAllocResp;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by antonis on 5/13/16.
 */
public class AllocRespTestUtils {

    private static final Log LOG = LogFactory.getLog(AllocRespTestUtils.class);

    public static Map<ApplicationAttemptId, AllocateResponse> populateAllocateResponses(int responseId,
            TransactionStateImpl ts, int numOfAllocResp, int numOfAllocCont, int numOfContStatuses) {
        Map<ApplicationAttemptId, AllocateResponse> allocateResponses =
                new HashMap<ApplicationAttemptId, AllocateResponse>(numOfAllocResp);
        for (int i = 0; i < numOfAllocResp; ++i) {
            ApplicationAttemptId appAtt = AllocRespTestUtils.createApplicationAttemptId(i);
            List<Container> allocatedContainers = new ArrayList<Container>(numOfAllocCont);
            for (int j = 0; j < numOfAllocCont; ++j) {
                allocatedContainers.add(AllocRespTestUtils.createContainer(appAtt, j));
            }

            List<ContainerStatus> completedContainers = new ArrayList<ContainerStatus>(numOfContStatuses);
            for (int j = 0; j < numOfContStatuses; ++j) {
                completedContainers.add(AllocRespTestUtils.createContainerStatus(appAtt, j));
            }

            AllocateResponse response = AllocRespTestUtils.createAllocateResponse(responseId, allocatedContainers,
                    completedContainers);
            allocateResponses.put(appAtt, response);

            ts.addAllocateResponse(appAtt,
                    new ApplicationMasterService.AllocateResponseLock(response));
        }

        return allocateResponses;
    }

    public static Container createContainer(ApplicationAttemptId appAttId, int containerId) {
        Container container = Container.newInstance(ContainerId.newInstance(appAttId, containerId),
                NodeId.newInstance("host0", 0), "host0",
                Resource.newInstance(1024 * 4, 4),
                Priority.newInstance(1),
                Token.newInstance("identifier".getBytes(), "someKind",
                        "password".getBytes(), "someService"));
        return container;
    }

    public static byte[] getContainerBytes(org.apache.hadoop.yarn.api.records.Container Container){
        if(Container instanceof ContainerPBImpl){
            return ((ContainerPBImpl) Container).getProto()
                    .toByteArray();
        }else{
            return new byte[0];
        }
    }

    public static ContainerStatus createContainerStatus(ApplicationAttemptId appAttId, int containerId) {
        return ContainerStatus.newInstance(
                ContainerId.newInstance(appAttId, containerId),
                ContainerState.COMPLETE,
                "GOOD",
                ContainerExitStatus.SUCCESS);
    }

    public static AllocateResponse createAllocateResponse(int responseId,
            List<Container> allocatedContainers, List<ContainerStatus> completedContainers) {
        return AllocateResponse.newInstance(responseId, completedContainers,
                allocatedContainers, new ArrayList<NodeReport>(),
                Resource.newInstance(1024 * 5, 5), AMCommand.AM_RESYNC,
                2, null, new ArrayList<NMToken>());
    }

    public static ApplicationAttemptId createApplicationAttemptId(int id) {
        return ApplicationAttemptId.newInstance(ApplicationId.newInstance(0L, id),
                id);
    }

    // Print the allocated containers response retrieved from DB
    public static void printAllocatedContainers(Map<String, List<io.hops.metadata.yarn.entity.rmstatestore.AllocateResponse>>
            allocConts) {
        for (Map.Entry<String, List<io.hops.metadata.yarn.entity.rmstatestore.AllocateResponse>> entry : allocConts.entrySet()) {
            String appAttID = entry.getKey();
            List<io.hops.metadata.yarn.entity.rmstatestore.AllocateResponse> responses = entry.getValue();
            LOG.info("Allocated containers for Attempt: " + appAttID);
            for (io.hops.metadata.yarn.entity.rmstatestore.AllocateResponse resp : responses) {
                LOG.info("Response ID: " + resp.getResponseId());
                for (String contId : resp.getAllocatedContainers()) {
                    LOG.info("Allocated container: " + contId);
                }
            }
        }
    }

    // Print the completed containers statuses retrieved from DB
    public static void printCompletedStatuses(Map<String, List<io.hops.metadata.yarn.entity.rmstatestore.AllocateResponse>>
            complCont) {
        for (Map.Entry<String, List<io.hops.metadata.yarn.entity.rmstatestore.AllocateResponse>> entry : complCont.entrySet()) {
            String appAttID = entry.getKey();
            List<io.hops.metadata.yarn.entity.rmstatestore.AllocateResponse> responses = entry.getValue();
            LOG.info("Completed statuses for Attempt: " + appAttID);
            for (io.hops.metadata.yarn.entity.rmstatestore.AllocateResponse resp : responses) {
                LOG.info("Response ID: " + resp.getResponseId());
                Map<String, byte[]> statuses = resp.getCompletedContainersStatus();
                for (Map.Entry<String, byte[]> status : statuses.entrySet()) {
                    LOG.info("Status for: " + status.getKey() + " is " + status.getValue());
                }
            }
        }
    }

    // Helper classes
    public static class AllocateResponsesFetcher extends LightWeightRequestHandler {

        public AllocateResponsesFetcher() {
            super(YARNOperationType.TEST);
        }

        @Override
        public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.readLock();

            AllocateResponseDataAccess allocRespDAO = (AllocateResponseDataAccess)
                    RMStorageFactory.getDataAccess(AllocateResponseDataAccess.class);

            Map<String, AllocateResponse> resultSet = allocRespDAO.getAll();

            connector.commit();

            return resultSet;
        }
    }

    public static class AllocatedContainersFetcher extends LightWeightRequestHandler {

        public AllocatedContainersFetcher() {
            super(YARNOperationType.TEST);
        }

        @Override
        public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.readLock();

            AllocatedContainersDataAccess allocContDAO = (AllocatedContainersDataAccess)
                    RMStorageFactory.getDataAccess(AllocatedContainersDataAccess.class);

            Map<String, List<io.hops.metadata.yarn.entity.rmstatestore.AllocateResponse>> resultSet =
                    allocContDAO.getAll();

            connector.commit();
            return resultSet;
        }
    }

    public static class CompletedContStatusFetcher extends LightWeightRequestHandler {

        public CompletedContStatusFetcher() {
            super(YARNOperationType.TEST);
        }

        @Override
        public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.readLock();

            CompletedContainersStatusDataAccess complDAO = (CompletedContainersStatusDataAccess)
                    RMStorageFactory.getDataAccess(CompletedContainersStatusDataAccess.class);

            Map<String, List<io.hops.metadata.yarn.entity.rmstatestore.AllocateResponse>> resultSet =
                    complDAO.getAll();

            connector.commit();

            return resultSet;
        }
    }

    public static class AllocRespGCFetcher extends LightWeightRequestHandler {

        public AllocRespGCFetcher() {
            super(YARNOperationType.TEST);
        }

        @Override
        public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.readLock();

            GarbageCollectorAllocRespDataAccess gcAllocDAO = (GarbageCollectorAllocRespDataAccess)
                    RMStorageFactory.getDataAccess(GarbageCollectorAllocRespDataAccess.class);

            List<GarbageCollectorAllocResp> resultSet = gcAllocDAO.getAll();
            connector.commit();

            return resultSet;
        }
    }

    public static class ContainerAdder extends LightWeightRequestHandler {

        private List<io.hops.metadata.yarn.entity.Container> toPersist;

        public ContainerAdder() {
            super(YARNOperationType.TEST);
        }

        public void setToPersistSet(List<io.hops.metadata.yarn.entity.Container> toPersist) {
            this.toPersist = toPersist;
        }

        @Override
        public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.writeLock();

            ContainerDataAccess contDAO = (ContainerDataAccess) RMStorageFactory
                    .getDataAccess(ContainerDataAccess.class);
            contDAO.addAll(toPersist);
            connector.commit();

            return null;
        }
    }
}
