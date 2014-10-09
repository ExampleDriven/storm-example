package org.exampledriven.stormexample.base;

import backtype.storm.ILocalDRPC;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.generated.StormTopology;

/**
 * Created by Peter_Szanto on 10/3/2014.
 */
public abstract class DrpcTopologyBuilder {

    public StormTopology buildStormRemoteTopology() {
        LinearDRPCTopologyBuilder builder = getLinearDRPCTopologyBuilder();

        return builder.createRemoteTopology();
    }

    public StormTopology buildStormLocalTopology(ILocalDRPC drpc) {
        LinearDRPCTopologyBuilder builder = getLinearDRPCTopologyBuilder();

        return builder.createLocalTopology(drpc);
    }

    protected abstract LinearDRPCTopologyBuilder getLinearDRPCTopologyBuilder();

    public abstract String getHandlerName();

    private static String getCurrentClassName() {
        return new Throwable() .getStackTrace()[0].getClassName();
    }

}
