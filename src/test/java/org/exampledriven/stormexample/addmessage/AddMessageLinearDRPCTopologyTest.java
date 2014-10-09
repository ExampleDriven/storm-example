package org.exampledriven.stormexample.addmessage;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;
import org.apache.thrift7.TException;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class AddMessageLinearDRPCTopologyTest {


    @Test(groups = "integration")
    public void testRemoteDrpc() throws TException, DRPCExecutionException {
        DRPCClient client = new DRPCClient("storm-server", 3772);
        String drpcResult = client.execute(new AddMessageLinearDRPCTopology().getHandlerName(), "hello");
        
        assertEquals("hello !", drpcResult);

    }

    @Test
    public void testLocalDrpc() throws Exception {

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        LocalDRPC drpc = new LocalDRPC();
        AddMessageLinearDRPCTopology addMessageLinearDRPCTopology = new AddMessageLinearDRPCTopology();
        cluster.submitTopology("drpc-demo", conf, addMessageLinearDRPCTopology.buildStormLocalTopology(drpc));

        String drpcResult = drpc.execute(addMessageLinearDRPCTopology.getHandlerName(), "hello");

        cluster.shutdown();
        drpc.shutdown();

        assertEquals(drpcResult, "[\"hello!++\",\"hello!!++\",\"hello!+\",\"hello!!+\"]");

    }

}
