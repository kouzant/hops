package org.apache.hadoop.yarn.server;

import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertNotNull;

import java.util.EnumSet;
import java.util.List;

/**
 * Created by antonis on 11/22/16.
 */
public class TestSSLServer {
    private final Log LOG = LogFactory.getLog(TestSSLServer.class);
    private MiniYARNCluster cluster;
    private Configuration conf;
    private ApplicationClientProtocol acClient;

    @Before
    public void setUp() throws Exception {
        conf = new YarnConfiguration();
        conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);
        conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_USE_RPC, true);
        conf.set(CommonConfigurationKeysPublic.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY,
                "org.apache.hadoop.net.HopsSSLSocketFactory");
        cluster = new MiniYARNCluster(TestSSLServer.class.getName(), 1, 1, 1, 1, false, true);
        cluster.init(conf);
        cluster.start();

        LOG.info("<Skata> Started cluster");
        acClient = ClientRMProxy.createRMProxy(conf, ApplicationClientProtocol.class, true);
    }

    @After
    public void tearDown() throws Exception {
        if (cluster != null) {
            cluster.stop();
        }

        if (acClient != null) {
            RPC.stopProxy(acClient);
        }
    }

    @Test(timeout = 5000)
    public void testRpcCall() throws Exception {
        EnumSet<NodeState> filter = EnumSet.of(NodeState.RUNNING);
        GetClusterNodesRequest req = GetClusterNodesRequest.newInstance();
        req.setNodeStates(filter);
        LOG.info("<Skata> Sending request");
        GetClusterNodesResponse res = acClient.getClusterNodes(req);
        LOG.info("<Skata> Got response from server");
        assertNotNull("Response should not be null", res);
        List<NodeReport> reports = res.getNodeReports();
        LOG.info("<Skata> Printing cluster nodes report");
        for (NodeReport report : reports) {
            LOG.info("<Skata> NodeId: " + report.getNodeId().getHost());
        }
    }
}
