/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.exampledriven;

import backtype.storm.Config;
import backtype.storm.ILocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class ExclamationTridentTopology {

    public static final String HANDLER_NAME = "trident-exclamation";

    public static TridentTopology newLocalDRPCTridentTopology(ILocalDRPC server) {

        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newDRPCStream(HANDLER_NAME, server);

        addSteps(stream);

        return topology;

    }

    public static TridentTopology newRemoteDRPCTridentTopology() {

        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newDRPCStream(HANDLER_NAME);

        addSteps(stream);

        return topology;

    }

    private static void addSteps(Stream stream) {

        stream.each(new Fields("args"), new AddExclamation(), new Fields("result"));

    }

    public static class AddExclamation extends BaseFunction {

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String param = tuple.getString(0);
            collector.emit(new Values(param + "!", param + "!!"));
        }

    }

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        Config conf = new Config();
        conf.setDebug(true);
        StormSubmitter.submitTopologyWithProgressBar("ExclamationTridentTopology", conf,
                ExclamationTridentTopology.newRemoteDRPCTridentTopology().build());
    }

}
