package org.exampledriven.base;

import backtype.storm.Config;
import backtype.storm.ILocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.generated.StormTopology;
import org.exampledriven.LinearDrpcTopologyBuilderExample;
import storm.trident.TridentTopology;

/**
 * Created by Peter_Szanto on 10/3/2014.
 */
public abstract class TridentTopologyBuilder {

//    public TridentTopology buildStormRemoteTopology() {
//        LinearDRPCTopologyBuilder builder = getLinearDRPCTopologyBuilder();
//
//        return builder.createRemoteTopology();
//    }
//
//    public TridentTopology buildStormLocalTopology(ILocalDRPC drpc) {
//        TridentTopology builder = getTridentTopologyBuilder();
//
//        return builder.newDRPCStream(getHandlerName(),drpc);
//    }
//
//    protected abstract TridentTopology getTridentTopologyBuilder();
//
//    public abstract String getHandlerName();
//
//    public static void main(String[] args) throws Exception {
//
//        Config conf = new Config();
//        conf.setDebug(true);
//
//        StormSubmitter.submitTopologyWithProgressBar(getCurrentClassName(), conf,
//                new LinearDrpcTopologyBuilderExample().buildStormRemoteTopology());
//    }
//
//    private static String getCurrentClassName() {
//        return new Throwable() .getStackTrace()[0].getClassName();
//    }

}
