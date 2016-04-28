package io.hops.ha.common;

import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.util.YarnAPIStorageFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by antonis on 4/27/16.
 */
public class TestGCService {

    private Configuration conf;

    @Before
    public void setup() throws Exception {
        conf = new YarnConfiguration();
        YarnAPIStorageFactory.setConfiguration(conf);
        RMStorageFactory.setConfiguration(conf);
        //RMUtilities.InitializeDB();
    }

    @Test
    public void testGC() throws Exception {
        MockRM rm = new MockRM(conf);
        rm.start();

        Thread.sleep(3000);
        rm.stop();

        Thread.sleep(1000);
    }
}
