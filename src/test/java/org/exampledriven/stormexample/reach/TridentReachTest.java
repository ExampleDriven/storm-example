package org.exampledriven.stormexample.reach;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;
import org.apache.thrift7.TException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TridentReachTest {

    @DataProvider(name = "url")
    public static Object[][] primeNumbers() {
        return new Object[][] {
//                { "[[16]]", "foo.com/blog/1" },
//                { "[[14]]", "engineering.twitter.com/blog/5" },
//                { "[[0]]",  "notaurl.com" },
                { "[[2]]",  "example.com" },
        };

    }

    @Test(dataProvider = "url", groups = "integration")
    public void testRemoteDrpc(String reach, String url) throws TException, DRPCExecutionException {

        DRPCClient client = new DRPCClient("storm-server", 3772);
        String drpcResult = client.execute(TridentReach.HANDLER_NAME, url);

        assertEquals(drpcResult, reach);

    }

    @Test(dataProvider = "url")
    public void testLocalDrpc(String reach, String url) throws Exception {

        Config conf = new Config();
        conf.setDebug(true);
        conf.setMessageTimeoutSecs(100000);

        conf.setMaxTaskParallelism(3);
        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("reach-drpc", conf, TridentReach.newLocalDRPCTridentTopology(drpc).build());

        assertEquals(drpc.execute(TridentReach.HANDLER_NAME, url), reach);
        assertEquals(drpc.execute(TridentReach.HANDLER_NAME, url), reach);

        cluster.shutdown();
        drpc.shutdown();

    }

}
