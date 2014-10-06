package org.exampledriven;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;
import org.apache.thrift7.TException;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class LinearDrpcTopologyExampleTest {


    @Test
    public void testRemoteDrpc() throws TException, DRPCExecutionException {
        DRPCClient client = new DRPCClient("evhubudsd6134.budapest.epam.com", 3772);
        String drpcResult = client.execute(new LinearDrpcTopologyBuilderExample().getHandlerName(), "hello");
        
        assertEquals("hello !", drpcResult);

    }

    @Test
    public void testLocalDrpc() throws Exception {

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        LocalDRPC drpc = new LocalDRPC();
        LinearDrpcTopologyBuilderExample drpcTopologyBuilderExample = new LinearDrpcTopologyBuilderExample();
        cluster.submitTopology("drpc-demo", conf, drpcTopologyBuilderExample.buildStormLocalTopology(drpc));

        String drpcResult = drpc.execute(drpcTopologyBuilderExample.getHandlerName(), "hello");

        cluster.shutdown();
        drpc.shutdown();

        assertEquals("hello !", drpcResult);

    }

}
