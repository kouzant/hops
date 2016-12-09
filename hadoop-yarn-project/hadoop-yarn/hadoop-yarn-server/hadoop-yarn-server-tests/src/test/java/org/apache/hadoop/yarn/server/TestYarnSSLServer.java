package org.apache.hadoop.yarn.server;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcServerException;
import org.apache.hadoop.net.HopsSSLSocketFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.net.ssl.SSLException;

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by antonis on 11/22/16.
 */
@RunWith(Parameterized.class)
public class TestYarnSSLServer {
    private final Log LOG = LogFactory.getLog(TestYarnSSLServer.class);

    private enum CERT_ERR {
        ERR_CN,
        NO_CA
    }

    @Parameterized.Parameters
    public static Collection parameters() {
        return Arrays.asList(new Object[][] {
                {CERT_ERR.ERR_CN},
                {CERT_ERR.NO_CA}
        });
    }

    private CERT_ERR error_mode = CERT_ERR.ERR_CN;
    private MiniYARNCluster cluster;
    private Configuration conf;
    private ApplicationClientProtocol acClient, acClient1;
    private Thread invoker;

    @Rule
    public final ExpectedException rule = ExpectedException.none();

    String passwd = "123456";
    String outDir;
    Path serverKeyStore, serverTrustStore;
    Path c_clientKeyStore, c_clientTrustStore;
    Path err_clientKeyStore, err_clientTrustStore;
    List<Path> filesToPurge;

    public TestYarnSSLServer(CERT_ERR error_mode) {
        this.error_mode = error_mode;
    }

    private List<Path> prepareCryptoMaterial() throws Exception {
        List<Path> filesToPurge = new ArrayList<>();

        String keyAlg = "RSA";
        String signAlg = "MD5withRSA";

        outDir = KeyStoreTestUtil.getClasspathDir(TestYarnSSLServer.class);

        // Generate CA
        KeyPair caKeyPair = KeyStoreTestUtil.generateKeyPair(keyAlg);
        X509Certificate caCert = KeyStoreTestUtil.generateCertificate("CN=CARoot", caKeyPair, 42, signAlg);

        // Generate server certificate signed by CA
        KeyPair serverKeyPair = KeyStoreTestUtil.generateKeyPair(keyAlg);
        X509Certificate serverCrt = KeyStoreTestUtil.generateSignedCertificate("CN=serverCrt", serverKeyPair, 42,
                signAlg, caKeyPair.getPrivate(), caCert);

        serverKeyStore = Paths.get(outDir, "server.keystore.jks");
        serverTrustStore = Paths.get(outDir, "server.truststore.jks");
        filesToPurge.add(serverKeyStore);
        filesToPurge.add(serverTrustStore);
        KeyStoreTestUtil.createKeyStore(serverKeyStore.toString(), passwd, passwd,
                "server_alias", serverKeyPair.getPrivate(), serverCrt);
        KeyStoreTestUtil.createTrustStore(serverTrustStore.toString(), passwd, "CARoot", caCert);

        // Generate client certificate with the correct CN field and signed by the CA
        KeyPair c_clientKeyPair = KeyStoreTestUtil.generateKeyPair(keyAlg);
        String c_cn = "CN=" + UserGroupInformation.getCurrentUser().getUserName();
        X509Certificate c_clientCrt = KeyStoreTestUtil.generateSignedCertificate(c_cn, c_clientKeyPair, 42,
                signAlg, caKeyPair.getPrivate(), caCert);

        c_clientKeyStore = Paths.get(outDir, "c_client.keystore.jks");
        c_clientTrustStore = Paths.get(outDir, "c_client.truststore.jks");
        filesToPurge.add(c_clientKeyStore);
        filesToPurge.add(c_clientTrustStore);
        KeyStoreTestUtil.createKeyStore(c_clientKeyStore.toString(), passwd, passwd,
                "c_client_alias", c_clientKeyPair.getPrivate(), c_clientCrt);
        KeyStoreTestUtil.createTrustStore(c_clientTrustStore.toString(), passwd, "CARoot", caCert);

        if (error_mode.equals(CERT_ERR.NO_CA)) {
            LOG.info("no ca error mode");
            // Generate client certificate with the correct CN field but NOT signed by the CA
            KeyPair noCA_clientKeyPair = KeyStoreTestUtil.generateKeyPair(keyAlg);
            X509Certificate noCA_clientCrt = KeyStoreTestUtil.generateCertificate(c_cn, noCA_clientKeyPair, 42,
                    signAlg);

            err_clientKeyStore = Paths.get(outDir, "noCA_client.keystore.jks");
            err_clientTrustStore = Paths.get(outDir, "noCA_client.truststore.jks");
            filesToPurge.add(err_clientKeyStore);
            filesToPurge.add(err_clientTrustStore);
            KeyStoreTestUtil.createKeyStore(err_clientKeyStore.toString(), passwd, passwd,
                    "noca_client_alias", noCA_clientKeyPair.getPrivate(), noCA_clientCrt);
            KeyStoreTestUtil.createTrustStore(err_clientTrustStore.toString(), passwd, "CARoot", caCert);

        } else if (error_mode.equals(CERT_ERR.ERR_CN)) {
            LOG.info("wrong cn error mode");
            // Generate client with INCORRECT CN field but signed by the CA
            KeyPair errCN_clientKeyPair = KeyStoreTestUtil.generateKeyPair(keyAlg);
            X509Certificate errCN_clientCrt = KeyStoreTestUtil.generateSignedCertificate("CN=Phil Lynott",
                    errCN_clientKeyPair, 42, signAlg, caKeyPair.getPrivate(), caCert);

            err_clientKeyStore = Paths.get(outDir, "errCN_client.keystore.jks");
            err_clientTrustStore = Paths.get(outDir, "errCN_client.truststore.jks");
            filesToPurge.add(err_clientKeyStore);
            filesToPurge.add(err_clientTrustStore);
            KeyStoreTestUtil.createKeyStore(err_clientKeyStore.toString(), passwd, passwd,
                    "errcn_client_alias", errCN_clientKeyPair.getPrivate(), errCN_clientCrt);
            KeyStoreTestUtil.createTrustStore(err_clientTrustStore.toString(), passwd, "CARoot", caCert);
        }

        return filesToPurge;
    }

    private void purgeFiles(List<Path> files) throws Exception {
        File file;
        for (Path path : files) {
            file = new File(path.toUri());
            if (file.exists()) {
                file.delete();
            }
        }
    }

    @Before
    public void setUp() throws Exception {
        filesToPurge = prepareCryptoMaterial();

        conf = new YarnConfiguration();
        conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);
        conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_USE_RPC, true);
        conf.set(CommonConfigurationKeysPublic.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY,
                "org.apache.hadoop.net.HopsSSLSocketFactory");
        conf.setBoolean(CommonConfigurationKeysPublic.IPC_SERVER_SSL_ENABLED, true);
        conf.set(SSLFactory.SSL_ENABLED_PROTOCOLS, "TLSv1.2");
        conf.set(SSLFactory.SSL_HOSTNAME_VERIFIER_KEY, "ALLOW_ALL");

        Configuration sslServerConf = KeyStoreTestUtil.createServerSSLConfig(serverKeyStore.toString(),
                passwd, passwd, serverTrustStore.toString(), passwd);
        Path sslServerPath = Paths.get(outDir, "ssl-server.xml");
        filesToPurge.add(sslServerPath);
        File sslServer = new File(sslServerPath.toUri());
        KeyStoreTestUtil.saveConfig(sslServer, sslServerConf);

        // Set the client certificate with correct CN and signed by the CA
        conf.set(HopsSSLSocketFactory.KEY_STORE_FILEPATH_KEY, c_clientKeyStore.toString());
        conf.set(HopsSSLSocketFactory.KEY_STORE_PASSWORD_KEY, passwd);
        conf.set(HopsSSLSocketFactory.KEY_PASSWORD_KEY, passwd);
        conf.set(HopsSSLSocketFactory.TRUST_STORE_FILEPATH_KEY, c_clientTrustStore.toString());
        conf.set(HopsSSLSocketFactory.TRUST_STORE_PASSWORD_KEY, passwd);

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

        purgeFiles(filesToPurge);
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
