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
package org.exampledriven.stormexample.addmessage.multiplestream;

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

/**
 * Similar to {@link org.exampledriven.stormexample.addmessage.singlestream.AddMessageStormTopology} but implemented with Trident
 */
public class AddMessageMultipleStreamTridentDRPCTopology {

    public static final String HANDLER_NAME = "trident-exclamation-plus";
    public static final String OUTPUT_1 = "ol";
    public static final String OUTPUT_2 = "o2";

    public static TridentTopology newLocalDRPCTridentTopology(ILocalDRPC server) {

        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newDRPCStream(HANDLER_NAME, server);

        addSteps(topology, stream);

        return topology;

    }

    public static TridentTopology newRemoteDRPCTridentTopology() {

        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newDRPCStream(HANDLER_NAME);

//        topology.newStre

        addSteps(topology, stream);

        return topology;

    }

    private static void addSteps(TridentTopology topology, Stream stream) {

        Stream stream1 = stream
            .each(new Fields("args"), new AddMessageFunction("!", "!!"), new Fields(OUTPUT_1, OUTPUT_2))
            .project(new Fields(OUTPUT_1, OUTPUT_2));

        Stream stream2 = stream
            .each(new Fields("args"), new AddMessageFunction("+", "++"),
                    new Fields(OUTPUT_1, OUTPUT_2)).parallelismHint(2)
            .project(new Fields(OUTPUT_1, OUTPUT_2));

        topology.merge(stream1, stream2);

    }

    public static class AddMessageFunction extends BaseFunction {

        private final String message1;
        private final String message2;

        public AddMessageFunction(String message1, String message2) {
            this.message1 = message1;
            this.message2 = message2;
        }

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String param = tuple.getString(0);
            collector.emit(new Values(param + message1, param + message2));
            collector.emit(new Values(param + message2, param + message1));
        }

    }

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        Config conf = new Config();
        conf.setDebug(true);
        StormSubmitter.submitTopologyWithProgressBar("ExclamationPlusTridentTopology", conf,
                AddMessageMultipleStreamTridentDRPCTopology.newRemoteDRPCTridentTopology().build());
    }

}
