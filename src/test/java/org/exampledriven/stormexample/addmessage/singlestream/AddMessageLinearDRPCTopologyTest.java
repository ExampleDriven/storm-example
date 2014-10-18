package org.exampledriven.stormexample.addmessage.singlestream;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import org.apache.thrift7.TException;
import org.testng.annotations.Test;
import java.util.HashSet;

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

        executeAndAssert(drpc, addMessageLinearDRPCTopology, "hello1");
        executeAndAssert(drpc, addMessageLinearDRPCTopology, "hello2");
        executeAndAssert(drpc, addMessageLinearDRPCTopology, "hello3");

        cluster.shutdown();
        drpc.shutdown();


    }

    public void executeAndAssert(LocalDRPC drpc, AddMessageLinearDRPCTopology addMessageLinearDRPCTopology, String param) {
        String drpcResult = drpc.execute(addMessageLinearDRPCTopology.getHandlerName(), param);
        HashSet drpcResultSet = new Gson().fromJson(drpcResult, HashSet.class);
        assertEquals(drpcResultSet, ImmutableSet.of(param + "!++", param + "!!++", param + "!+", param + "!!+"));
    }

}
