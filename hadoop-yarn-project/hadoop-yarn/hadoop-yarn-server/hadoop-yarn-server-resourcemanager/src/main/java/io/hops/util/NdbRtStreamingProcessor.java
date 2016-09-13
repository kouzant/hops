package io.hops.util;

import io.hops.metadata.yarn.entity.ContainerStatus;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImplDist;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRMDist;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManagerDist;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.util.List;
import java.util.Set;

/**
 * Created by antonis on 8/22/16.
 */
public class NdbRtStreamingProcessor extends NdbStreamingReceiver {

    private RMNode rmNode;

    public NdbRtStreamingProcessor(RMContext rmContext) {
        super(rmContext, "RT Event retriever");
        setRetrievingRunnable(new RetrievingThread());
    }

    public void printStreamingRTComps(StreamingRTComps streamingRTComps) {
        for (String streamingRTCompsNodeId : streamingRTComps.getNodeIds()) {
            LOG.debug("============= " + streamingRTCompsNodeId);
            List<ApplicationId> applicationIdList = streamingRTComps
                    .getFinishedAppsByNodeId(streamingRTCompsNodeId);
            if (applicationIdList != null) {
                for (ApplicationId appId : applicationIdList) {
                    LOG.debug("<Processor> Finished application : appId: " + appId.toString()
                            + " nodeId: " + streamingRTCompsNodeId);
                }
            }

            Set<ContainerId> containerIdList = streamingRTComps.getContainersToCleanByNodeId(streamingRTCompsNodeId);
            if (containerIdList != null) {
                for (ContainerId cid : containerIdList) {
                    LOG.debug("<Processor> Containers to clean containerId: " + cid.toString());
                }
            }

            if (streamingRTComps.isNextHeartbeatForNodeId(streamingRTCompsNodeId) != null) {
                LOG.debug("<Processor> RTReceived: " + streamingRTCompsNodeId + " nextHeartbeat: "
                        + streamingRTComps.isNextHeartbeatForNodeId(streamingRTCompsNodeId));
            }
        }
    }
    
    private class RetrievingThread implements Runnable {
        int numOfEvents = 0;
        long lastTimestamp = 0;

        @Override
        public void run() {
            while (running) {
                if (!rmContext.isLeader()) {
                    try {
                        StreamingRTComps streamingRTComps = null;
                        streamingRTComps = NdbRtStreamingReceiver.receivedRTEvents
                                .take();

                        if (streamingRTComps != null) {
                            if (LOG.isDebugEnabled()) {
                                printStreamingRTComps(streamingRTComps);
                            }

                            if (streamingRTComps.getNodeIds() != null) {
                                for (String streamingRTCompsNodeId : streamingRTComps.getNodeIds()) {
                                    long totalStart = System.currentTimeMillis();
                                    NodeId nodeId = ConverterUtils.toNodeId(streamingRTCompsNodeId);
                                    rmNode = rmContext.getRMNodes().get(nodeId);

                                    if (rmNode != null) {
                                        //long start = System.currentTimeMillis();
                                        if (streamingRTComps
                                                .getContainersToCleanByNodeId(streamingRTCompsNodeId) != null) {
                                            ((RMNodeImplDist) rmNode).setContainersToCleanUp(
                                                    streamingRTComps.getContainersToCleanByNodeId(
                                                            streamingRTCompsNodeId));
                                        }

                                        if (streamingRTComps
                                                .getFinishedAppsByNodeId(streamingRTCompsNodeId) != null) {
                                            ((RMNodeImplDist) rmNode).setAppsToCleanUp(streamingRTComps
                                                    .getFinishedAppsByNodeId(streamingRTCompsNodeId));
                                        }

                                        if (streamingRTComps.isNextHeartbeatForNodeId(
                                                streamingRTCompsNodeId) != null) {
                                            ((RMNodeImplDist) rmNode).setNextHeartbeat(streamingRTComps
                                                    .isNextHeartbeatForNodeId(streamingRTCompsNodeId));
                                        }
                                        /*long diff = System.currentTimeMillis() - start;
                                        if (diff > 10) {
                                            LOG.error(">>> Updating RMNode too long: " + diff);
                                        }*/
                                    } else {
                                        LOG.warn(">>> RMNode is NULL");
                                    }

                                    /*long totalDiff = System.currentTimeMillis() - totalStart;
                                    if (totalDiff > 20) {
                                        LOG.error(">>> Processing RMNode too long: " + totalDiff);
                                    }*/
                                }
                            }

                            if (streamingRTComps.getCurrentNMMasterKey() != null) {
                                //long start = System.currentTimeMillis();
                                ((NMTokenSecretManagerInRMDist) rmContext.getNMTokenSecretManager())
                                        .setCurrentMasterKey(streamingRTComps.getCurrentNMMasterKey());
                                /*long diff = System.currentTimeMillis() - start;
                                if (diff > 5) {
                                    LOG.error(">>> Updating NM Current Master Key too long: " + diff);
                                }*/
                            }

                            if (streamingRTComps.getNextNMMasterKey() != null) {
                                //long start = System.currentTimeMillis();
                                ((NMTokenSecretManagerInRMDist) rmContext.getNMTokenSecretManager())
                                        .setNextMasterKey(streamingRTComps.getNextNMMasterKey());
                                /*long diff = System.currentTimeMillis() - start;
                                if (diff > 5) {
                                    LOG.error(">> Updating NM Next Master Key too long: " + diff);
                                }*/
                            }

                            if (streamingRTComps.getCurrentRMContainerMasterKey() != null) {
                                //long start = System.currentTimeMillis();
                                ((RMContainerTokenSecretManagerDist) rmContext.getContainerTokenSecretManager())
                                        .setCurrentMasterKey(streamingRTComps.getCurrentRMContainerMasterKey());
                                /*long diff = System.currentTimeMillis() - start;
                                if (diff > 5) {
                                    LOG.error(">>> Updating Container Master Key too long: " + diff);
                                }*/
                            }

                            if (streamingRTComps.getNextRMContainerMasterKey() != null) {
                                //long start = System.currentTimeMillis();
                                ((RMContainerTokenSecretManagerDist) rmContext.getContainerTokenSecretManager())
                                        .setNextMasterKey(streamingRTComps.getNextRMContainerMasterKey());
                                /*long diff = System.currentTimeMillis() - start;
                                if (diff > 5) {
                                    LOG.error(">>> Updating Container Next Master Key too long: " + diff);
                                }*/
                            }

                            //numOfEvents++;

                            /*if ((System.currentTimeMillis() - lastTimestamp) >= 1000) {
                                LOG.error("*** <Profiler> Processed " + numOfEvents + " per second");
                                numOfEvents = 0;
                                lastTimestamp = System.currentTimeMillis();
                                LOG.error(">>> Persisted Pending Events per second: " + DBUtility.rtPe.getAndSet(0));
                            }*/
                        }
                    } catch (InterruptedException ex) {
                        LOG.error(ex, ex);
                    }
                }
            }
            LOG.info("HOP :: RT Event retriever interrupted");
        }
    }
}
