package org.apache.hadoop.yarn.server.resourcemanager;


import io.hops.ha.common.TransactionState;
import io.hops.ha.common.TransactionStateImpl;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.util.YarnAPIStorageFactory;
import io.hops.metadata.yarn.entity.YarnApplicationResources;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TestAppResourcesPersistence {

    private static final Log LOG = LogFactory.getLog(TestAppResourcesPersistence.class);
    YarnApplicationResources ap;
    YarnApplicationResources ap1;
    YarnApplicationResources ap2;

    @Before
    public void setUp() throws IOException {
        Configuration conf = new YarnConfiguration();
        YarnAPIStorageFactory.setConfiguration(conf);
        RMStorageFactory.setConfiguration(conf);
        RMUtilities.InitializeDB();
        ap = new YarnApplicationResources(12, "asd", 512, 1);
        ap1 = new YarnApplicationResources(132, "hi", 1024, 1);
        ap2 = new YarnApplicationResources(124, "something", 512, 2);
    }

    @Test
    public void TestRecord() throws IOException, InterruptedException {
        TransactionStateImpl transactionState = new TransactionStateImpl(TransactionState.TransactionType.RM);
        transactionState.addYarnApplicationResourcesToAdd(ap);
        transactionState.addYarnApplicationResourcesToAdd(ap1);
        transactionState.addYarnApplicationResourcesToAdd(ap2);
        transactionState.decCounter(TransactionState.TransactionType.RM);
        Thread.sleep(1000);
    }

    @Test (timeout = 5000)
    public void TestRemove() throws IOException, InterruptedException {
        TransactionStateImpl transactionState = new TransactionStateImpl(TransactionState.TransactionType.RM);
        transactionState.addYarnApplicationResourcesToRemove(ap);
        transactionState.decCounter(TransactionState.TransactionType.RM);
        Thread.sleep(1000);
    }
}
