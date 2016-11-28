package org.apache.hadoop.yarn.server;

import org.apache.commons.io.IOExceptionWithCause;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.HopsSSLSocketFactory;
import org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.net.ssl.SSLHandshakeException;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by antonis on 11/22/16.
 */
public class TestSSLServer {
    private final Log LOG = LogFactory.getLog(TestSSLServer.class);
    private MiniYARNCluster cluster;
    private Configuration conf;
    private ApplicationClientProtocol acClient, acClient1;

    @Before
    public void setUp() throws Exception {
        conf = new YarnConfiguration();
        conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);
        conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_USE_RPC, true);
        conf.set(CommonConfigurationKeysPublic.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY,
                "org.apache.hadoop.net.HopsSSLSocketFactory");
        conf.setBoolean(CommonConfigurationKeysPublic.IPC_SERVER_SSL_ENABLED, true);
        conf.set(SSLFactory.SSL_ENABLED_PROTOCOLS, "TLSv1.2");
        conf.set(SSLFactory.SSL_HOSTNAME_VERIFIER_KEY, "ALLOW_ALL");

        cluster = new MiniYARNCluster(TestSSLServer.class.getName(), 1, 3, 1, 1, false, true);
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

        if (acClient1 != null) {
            RPC.stopProxy(acClient1);
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
            LOG.info("<Skata> NodeId: " + report.getNodeId().toString());
        }
    }

    @Test
    public void testRpcCallWithSecondClient() throws Exception {
        conf.set(HopsSSLSocketFactory.KEY_STORE_FILEPATH_KEY,
                "/home/antonis/SICS/key_material/client2.keystore.jks");
        conf.set(HopsSSLSocketFactory.KEY_STORE_PASSWORD_KEY,
                "1234567");
        conf.set(HopsSSLSocketFactory.KEY_PASSWORD_KEY,
                "1234567");
        conf.set(HopsSSLSocketFactory.TRUST_STORE_FILEPATH_KEY,
                "/home/antonis/SICS/key_material/client2.truststore.jks");
        conf.set(HopsSSLSocketFactory.TRUST_STORE_PASSWORD_KEY,
                "1234567");

        LOG.info("Creating the second client");
        acClient1 = ClientRMProxy.createRMProxy(conf, ApplicationClientProtocol.class,
                true);

        EnumSet<NodeState> filter = EnumSet.of(NodeState.RUNNING);
        GetClusterNodesRequest req = GetClusterNodesRequest.newInstance();
        req.setNodeStates(filter);
        GetClusterNodesResponse res = acClient.getClusterNodes(req);
        assertNotNull("Response from the first client should not be null", res);

        Thread invoker = new Thread(new Invoker(acClient));
        invoker.start();

        GetNewApplicationRequest req1 = GetNewApplicationRequest.newInstance();
        LOG.info("Sending request from the second client");
        GetNewApplicationResponse res1 = acClient1.getNewApplication(req1);

        invoker.join();
    }

    private class Invoker implements Runnable {

        private final ApplicationClientProtocol acClient;

        public Invoker(ApplicationClientProtocol acClient) {
            this.acClient = acClient;
        }

        @Override
        public void run() {
            LOG.info("Sending request from the Invoker thread");
            try {
                TimeUnit.MILLISECONDS.sleep(500);
                GetNewApplicationResponse res = acClient.getNewApplication(GetNewApplicationRequest.newInstance());
                assertNotNull(res);

                LOG.info("New application id for invoker is: " + res.getApplicationId());
            } catch (YarnException | IOException | InterruptedException ex) {
                LOG.error(ex, ex);
            }
        }
    }
}
