package io.hops.metadata.util;


import io.hops.metadata.yarn.entity.appmasterrpc.RPC;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TestApplicationResourcesContainers {

    private static Log LOG = LogFactory.getLog(TestApplicationResourcesContainers.class);
    private YarnConfiguration conf;
    private final int GB = 1024;
    Map<String, Resource> appResources = new HashMap<String, Resource>();

    @Before
    public void setup() throws IOException {
        LOG.info("Setting up Factories");
        conf = new YarnConfiguration();
        conf.set(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, "4000");
        YarnAPIStorageFactory.setConfiguration(conf);
        RMStorageFactory.setConfiguration(conf);
        RMUtilities.InitializeDB();
        conf.setClass(YarnConfiguration.RM_SCHEDULER,
                FifoScheduler.class, ResourceScheduler.class);
    }

    @Test(timeout = 30000)
    public void testRPCPersistence() throws IOException {
        int rpcID = HopYarnAPIUtilities.getRPCID();
        RPC.Type type = RPC.Type.RegisterApplicationMaster;
        byte[] array = type.toString().getBytes();
        RMUtilities.persistAppMasterRPC(rpcID, type, array);

        rpcID = HopYarnAPIUtilities.getRPCID();
        type = RPC.Type.FinishApplicationMaster;
        array = type.toString().getBytes();
        RMUtilities.persistAppMasterRPC(rpcID, type, array);

        rpcID = HopYarnAPIUtilities.getRPCID();
        type = RPC.Type.Allocate;
        array = type.toString().getBytes();
        RMUtilities.persistAppMasterRPC(rpcID, type, array);

    }

    @Test(timeout = 80000)
    public void random() throws Exception {
        MockRM rm = new MockRM(conf);
        rm.start();

        //Register one NodeManager
        LOG.info("Register one Nodemanager with 10 GB memory");
        MockNM nm1 = rm.registerNode("127.0.0.1:1234", 10 * GB);

        // Submit 3 applications
        LOG.info("Submit 1st application with 3GB memory");
        RMApp app1 = rm.submitApp(2 * GB, "app_1", "user_1", null, "queue1");
        LOG.info("Submit 2nd application with 3GB memory");
        RMApp app2 = rm.submitApp(GB, "app_2", "user_2", null, "queue2");
        LOG.info("Submit 3rd application with 2GB memory");
        RMApp app3 = rm.submitApp(GB, "app_3", "user_3", null, "queue3");

        /**
         * ************************
         * THIS TEST PASSES AS IT IS IF WE INCREASE THE MEMORY OF ONE OF THE 3 APPS,
         * THEN IT WILL FAIL AS THE
         * ENTIRE CLUSTER CAPACITY IS 10 GB
         * APPATTEMPT STATE WILL NOT BE ALLOCATED AND AN EXCEPTION WILL BE THROWN
         *************************
         */
        nm1.nodeHeartbeat(true);

        RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
        RMAppAttempt attempt2 = app2.getCurrentAppAttempt();
        RMAppAttempt attempt3 = app3.getCurrentAppAttempt();

        appResources.put(app1.getName(), app1.getApplicationSubmissionContext().getResource());
        appResources.put(app2.getName(), app2.getApplicationSubmissionContext().getResource());
        appResources.put(app3.getName(), app3.getApplicationSubmissionContext().getResource());

        MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
        am1.registerAppAttempt();

        MockAM am2 = rm.sendAMLaunched(attempt2.getAppAttemptId());
        am2.registerAppAttempt();

        MockAM am3 = rm.sendAMLaunched(attempt3.getAppAttemptId());
        am3.registerAppAttempt();

        Thread.sleep(2000);

        // AM_1 sends request allocation - 2 containers with 1 GB each
        am1.addRequests(new String[]{"127.0.0.1"}, 2048, 1, 2);
        AllocateResponse alloc1Response = am1.schedule(); // send the request

        // AM_2 sends request allocation - 1 containers with 1 GB
        am2.addRequests(new String[]{"127.0.0.1"}, 1024, 0, 1);
        AllocateResponse alloc2Response = am2.schedule(); // send the request

        nm1.nodeHeartbeat(true);

        while (alloc1Response.getAllocatedContainers().size() < 1) {
            LOG.info("Waiting for containers to be created for app 1...");
            Thread.sleep(1000);
            alloc1Response = am1.schedule();
        }

        while (alloc2Response.getAllocatedContainers().size() < 1) {
            LOG.info("Waiting for containers to be created for app 2...");
            Thread.sleep(1000);
            alloc2Response = am2.schedule();
        }

        nm1.nodeHeartbeat(true);
        Thread.sleep(1000);

        rm.stop();
        Thread.sleep(2000);

    }
}
