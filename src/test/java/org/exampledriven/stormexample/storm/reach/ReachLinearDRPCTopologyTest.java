package org.exampledriven.stormexample.storm.reach;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import org.exampledriven.stormexample.storm.reach.ReachLinearDRPCTopology;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class ReachLinearDRPCTopologyTest {

    @DataProvider(name = "url")
    public static Object[][] primeNumbers() {
        return new Object[][] {
                { 16, "foo.com/blog/1" },
                { 14, "engineering.twitter.com/blog/5" },
                { 0, "notaurl.com" },
                { 2, "example.com" },
                };

    }

    @Test(dataProvider = "url")
    public void testLocalDrpc(Integer reach, String url) throws Exception {

        Config conf = new Config();
        conf.setDebug(true);
        conf.setMessageTimeoutSecs(100000);

        conf.setMaxTaskParallelism(3);
        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();
        ReachLinearDRPCTopology reachTopology = new ReachLinearDRPCTopology();

        cluster.submitTopology("reach-drpc", conf, reachTopology.buildStormLocalTopology(drpc));

        assertEquals((Integer)Integer.parseInt(drpc.execute(reachTopology.getHandlerName(), url)), reach);

        cluster.shutdown();
        drpc.shutdown();

    }

}
