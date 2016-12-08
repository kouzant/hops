package org.apache.hadoop.hdfs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.HopsSSLSocketFactory;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.net.ssl.SSLException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * Created by antonis on 12/7/16.
 */
public class TestDFSSSLServer {
    @Rule
    public final ExpectedException rule = ExpectedException.none();
    private final Log LOG = LogFactory.getLog(TestDFSSSLServer.class);

    MiniDFSCluster cluster;
    FileSystem dfs1, dfs2;
    Configuration conf;
    Thread invoker;

    @Before
    public void setUp() throws Exception {
        conf = new HdfsConfiguration();
        conf.set(CommonConfigurationKeysPublic.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY,
                "org.apache.hadoop.net.HopsSSLSocketFactory");
        conf.setBoolean(CommonConfigurationKeysPublic.IPC_SERVER_SSL_ENABLED, true);

        conf.set(SSLFactory.SSL_ENABLED_PROTOCOLS, "TLSv1.2");
        conf.set(SSLFactory.SSL_HOSTNAME_VERIFIER_KEY, "ALLOW_ALL");

        // Set the client certificate with the correct CN, antonis
        conf.set(HopsSSLSocketFactory.KEY_STORE_FILEPATH_KEY,
                "/home/antonis/SICS/key_material/cl_antonis.keystore.jks");
        conf.set(HopsSSLSocketFactory.KEY_STORE_PASSWORD_KEY, "123456");
        conf.set(HopsSSLSocketFactory.KEY_PASSWORD_KEY, "123456");
        conf.set(HopsSSLSocketFactory.TRUST_STORE_FILEPATH_KEY,
                "/home/antonis/SICS/key_material/cl_antonis.truststore.jks");
        conf.set(HopsSSLSocketFactory.TRUST_STORE_PASSWORD_KEY, "123456");

        String testDataPath = System
                .getProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA, "build/test/data");
        File testDataCluster1 = new File(testDataPath, "dfs_cluster");
        String c1Path = testDataCluster1.getAbsolutePath();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, c1Path);

        cluster = new MiniDFSCluster.Builder(conf).build();
    }

    @After
    public void tearDown() throws Exception {
        if (invoker != null) {
            invoker.join();
            invoker = null;
        }

        if (cluster != null) {
            cluster.shutdown();
        }

        if (dfs1 != null) {
            dfs1.close();
        }

        if (dfs2 != null) {
            dfs2.close();
        }
    }

    @Test
    public void testRpcCall() throws Exception {
        LOG.debug("testRpcCall");
        dfs1 = DistributedFileSystem.newInstance(conf);
        boolean exists = dfs1.exists(new Path("some_path"));
        LOG.debug("Does exist? " + exists);
        assertFalse(exists);
    }

    @Test
    public void testRpcCallNonValidCert() throws Exception {
        dfs1 = DistributedFileSystem.newInstance(conf);

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

        // Exception will be thrown later. JUnit does not execute the code
        // after the exception, so make the call in a separate thread
        invoker = new Thread(new Invoker(dfs1));
        invoker.start();

        rule.expect(SSLException.class);
        dfs2 = DistributedFileSystem.newInstance(conf);
    }

    private class Invoker implements Runnable {
        private final FileSystem dfs;

        public Invoker(FileSystem dfs) {
            this.dfs = dfs;
        }

        @Override
        public void run() {
            try {
                TimeUnit.SECONDS.sleep(1);
                LOG.debug("Making RPC call from the correct client");
                Path file = new Path("some_file");
                dfs.create(file);
                boolean exists = dfs.exists(file);
                assertTrue("File: " + file.getName() + " should have been created", exists);
            } catch (Exception ex) {
                LOG.error(ex, ex);
            }
        }
    }
}
