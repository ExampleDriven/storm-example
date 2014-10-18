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
package org.exampledriven.stormexample.addmessage.singlestream;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.TopologyBuilder;
import org.exampledriven.stormexample.addmessage.bolt.AddMessageBolt;

/**
 * Similar to {@link AddMessageTridentDRPCTopology} but implemented with storm
 */
public class AddMessageStormTopology {

    public static final String WORD_SPOUT = "word spout";
    public static final String EXCLAIM = "exclaim";
    public static final String PLUS = "plus";

    public static TopologyBuilder newTopology() {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(WORD_SPOUT, new TestWordSpout(), 10);
        builder.setBolt(EXCLAIM, new AddMessageBolt("!", "!!"), 5).shuffleGrouping(WORD_SPOUT);
        builder.setBolt(PLUS, new AddMessageBolt("+", "++"), 3).shuffleGrouping(EXCLAIM);

        return builder;
    }

    public static void main(String[] args) throws Exception {

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(3);

        StormSubmitter.submitTopologyWithProgressBar(args[0], conf, newTopology().createTopology());

    }
}
