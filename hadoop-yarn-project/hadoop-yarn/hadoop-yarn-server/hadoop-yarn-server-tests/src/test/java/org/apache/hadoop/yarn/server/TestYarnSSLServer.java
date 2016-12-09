package org.apache.hadoop.yarn.server;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcServerException;
import org.apache.hadoop.net.HopsSSLSocketFactory;
import org.apache.hadoop.security.ssl.HopsSSLTestUtils;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.net.ssl.SSLException;

import static org.junit.Assert.assertNotNull;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by antonis on 11/22/16.
 */
@RunWith(Parameterized.class)
public class TestYarnSSLServer extends HopsSSLTestUtils {
    private final Log LOG = LogFactory.getLog(TestYarnSSLServer.class);

    private MiniYARNCluster cluster;
    private Configuration conf;
    private ApplicationClientProtocol acClient, acClient1;

    public TestYarnSSLServer(CERT_ERR error_mode) {
        super.error_mode = error_mode;
    }

    @Before
    public void setUp() throws Exception {
        LOG.debug("Error mode: " + error_mode.name());

        filesToPurge = prepareCryptoMaterial();
        conf = new YarnConfiguration();
        conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);
        conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_USE_RPC, true);

        setCryptoConfig(conf);

        cluster = new MiniYARNCluster(TestYarnSSLServer.class.getName(), 1, 3, 1, 1, false, true);
        cluster.init(conf);
        cluster.start();

        LOG.info("Started cluster");
        acClient = ClientRMProxy.createRMProxy(conf, ApplicationClientProtocol.class, true);
    }

    @After
    public void tearDown() throws Exception {
        if (invoker != null) {
            invoker.join();
            invoker = null;
        }
        if (cluster != null) {
            LOG.info("Stopping MiniYARN cluster");
            cluster.stop();
        }

        if (acClient != null) {
            RPC.stopProxy(acClient);
        }

        if (acClient1 != null) {
            RPC.stopProxy(acClient1);
        }
    }

    @Test(timeout = 3000)
    public void testRpcCall() throws Exception {
        EnumSet<NodeState> filter = EnumSet.of(NodeState.RUNNING);
        GetClusterNodesRequest req = GetClusterNodesRequest.newInstance();
        req.setNodeStates(filter);
        LOG.debug("Sending request");
        GetClusterNodesResponse res = acClient.getClusterNodes(req);
        LOG.debug("Got response from server");
        assertNotNull("Response should not be null", res);
        List<NodeReport> reports = res.getNodeReports();
        LOG.debug("Printing cluster nodes report");
        for (NodeReport report : reports) {
            LOG.debug("NodeId: " + report.getNodeId().toString());
        }
    }

    @Test
    public void testRpcCallWithNonValidCert() throws Exception {
        conf.set(HopsSSLSocketFactory.KEY_STORE_FILEPATH_KEY, err_clientKeyStore.toString());
        conf.set(HopsSSLSocketFactory.KEY_STORE_PASSWORD_KEY, passwd);
        conf.set(HopsSSLSocketFactory.KEY_PASSWORD_KEY, passwd);
        conf.set(HopsSSLSocketFactory.TRUST_STORE_FILEPATH_KEY, err_clientTrustStore.toString());
        conf.set(HopsSSLSocketFactory.TRUST_STORE_PASSWORD_KEY, passwd);

        // Exception will be thrown later. JUnit does not execute the code
        // after the exception, so make the call in a separate thread
        invoker = new Thread(new Invoker(acClient));
        invoker.start();

        LOG.debug("Creating the second client");
        acClient1 = ClientRMProxy.createRMProxy(conf, ApplicationClientProtocol.class,
                true);

        GetNewApplicationRequest req1 = GetNewApplicationRequest.newInstance();
        if (error_mode.equals(CERT_ERR.NO_CA)) {
            rule.expect(SSLException.class);
        } else if (error_mode.equals(CERT_ERR.ERR_CN)) {
            rule.expect(RpcServerException.class);
        }
        GetNewApplicationResponse res1 = acClient1.getNewApplication(req1);
    }

    private class Invoker implements Runnable {
        private final ApplicationClientProtocol client;

        public Invoker(ApplicationClientProtocol client) {
            this.client = client;
        }

        @Override
        public void run() {
            EnumSet<NodeState> filter = EnumSet.of(NodeState.RUNNING);
            GetClusterNodesRequest req = GetClusterNodesRequest.newInstance();
            req.setNodeStates(filter);
            LOG.debug("Sending cluster nodes request from first client");
            try {
                TimeUnit.SECONDS.sleep(1);
                GetClusterNodesResponse res = client.getClusterNodes(req);
                assertNotNull("Response from the first client should not be null", res);
                LOG.debug("NodeReports: " + res.getNodeReports().size());
                for (NodeReport nodeReport : res.getNodeReports()) {
                    LOG.debug("Node: " + nodeReport.getNodeId() + " Capability: " + nodeReport.getCapability());
                }
            } catch (Exception ex) {
                LOG.error(ex, ex);
            }
        }
    }
}
