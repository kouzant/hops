package io.hops.ha.common;

import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.util.YarnAPIStorageFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by antonis on 5/13/16.
 */
public class MicrobenchAllocResponses {

    private final Log LOG = LogFactory.getLog(MicrobenchAllocResponses.class);

    private Configuration conf;
    private final int NUM_OF_ALLOC = 100;
    private final int NUM_OF_ALLOC_CONTAINERS = 100;
    private final int NUM_OF_CONTAINER_STATUSES = 100;

    @Before
    public void setup() throws Exception {
        conf = new YarnConfiguration();
        YarnAPIStorageFactory.setConfiguration(conf);
        RMStorageFactory.setConfiguration(conf);
        RMUtilities.InitializeDB();
    }

    @Test
    public void measureAllocRespAdd() throws Exception {
        TransactionStateImpl ts0 = new TransactionStateImpl(TransactionState.TransactionType.APP);
        AllocRespTestUtils.populateAllocateResponses(0, ts0, NUM_OF_ALLOC, NUM_OF_ALLOC_CONTAINERS,
                NUM_OF_CONTAINER_STATUSES);
        LOG.info("Committing first Allocate Responses");
        ts0.decCounter(TransactionState.TransactionType.APP);
        Thread.sleep(6000);

        // Populate twice so that we measure the time taken to remove old values
        TransactionStateImpl ts1 = new TransactionStateImpl(TransactionState.TransactionType.APP);
        AllocRespTestUtils.populateAllocateResponses(1, ts1, NUM_OF_ALLOC, NUM_OF_ALLOC_CONTAINERS,
                NUM_OF_CONTAINER_STATUSES);
        LOG.info("Committing second Allocate Responses");
        ts1.decCounter(TransactionState.TransactionType.APP);
        Thread.sleep(6000);
    }
}
