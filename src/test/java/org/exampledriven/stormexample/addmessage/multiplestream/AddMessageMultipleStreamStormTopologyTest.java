package org.exampledriven.stormexample.addmessage.multiplestream;

import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.LocalCluster;
import backtype.storm.utils.Utils;
import org.exampledriven.stormexample.addmessage.multiplestream.AddMessageMultipleStreamStormTopology;
import org.testng.annotations.Test;

public class AddMessageMultipleStreamStormTopologyTest {

    @Test
    public void testLocalTopology() throws Exception {

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(3);

        ILocalCluster cluster = new LocalCluster();

        cluster.submitTopology("test", conf, AddMessageMultipleStreamStormTopology.newTopology().createTopology());
        Utils.sleep(3000);
        cluster.killTopology("test");
        cluster.shutdown();


    }
}
