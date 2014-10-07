package org.exampledriven;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;
import org.apache.thrift7.TException;
import org.testng.annotations.Test;

import static org.testng.Assert.*;
import static org.testng.Assert.assertEquals;

public class ExclamationTridentTopologyTest {

    @Test(groups = "integration")
    public void testRemoteDrpc(String reach, String url) throws TException, DRPCExecutionException {

        DRPCClient client = new DRPCClient("storm-server", 3772);
        String drpcResult = client.execute(ExclamationTridentTopology.HANDLER_NAME, url);

        assertEquals(drpcResult, reach);

    }

    @Test
    public void testLocalDrpc() throws Exception {

        Config conf = new Config();
        conf.setDebug(true);
        conf.setMessageTimeoutSecs(100000);

        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("test-topology", conf, ExclamationTridentTopology.newLocalDRPCTridentTopology(drpc).build());

        String drpcResult = drpc.execute(ExclamationTridentTopology.HANDLER_NAME, "hello");
        String expectedResult = "[[\"hello\",\"hello!\"]]";
        assertEquals(drpcResult, expectedResult);

        cluster.shutdown();
        drpc.shutdown();

    }
}
