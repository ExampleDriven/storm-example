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
import backtype.storm.StormSubmitter;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.TopologyBuilder;
import org.exampledriven.stormexample.addmessage.bolt.AddMessageBolt;
import org.exampledriven.stormexample.addmessage.bolt.StreamSplitterBolt;

/**
 * Similar to {@link org.exampledriven.stormexample.addmessage.singlestream.AddMessageTridentDRPCTopology} but implemented with storm
 */
public class AddMessageMultipleStreamStormTopology {

    public static final String WORD_SPOUT = "word spout";
    public static final String EXCLAIM1 = "exclaim";
    public static final String EXCLAIM2 = "exclaim2";
    public static final String PLUS1 = "plus1";
    public static final String PLUS2 = "plus2";
    public static final String STREAM2 = "stream2";
    public static final String STREAM1 = "stream1";
    private static final String SPLITTER = "splitter";

    public static TopologyBuilder newTopology() {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(WORD_SPOUT, new TestWordSpout(), 10);
        builder.setBolt(SPLITTER, new StreamSplitterBolt(STREAM1, STREAM2), 5).shuffleGrouping(WORD_SPOUT);

        builder.setBolt(EXCLAIM1, new AddMessageBolt("1!", "1!!"), 5).shuffleGrouping(SPLITTER, STREAM1);
        builder.setBolt(PLUS1, new AddMessageBolt("1+", "1++"), 3).shuffleGrouping(EXCLAIM1);

        builder.setBolt(EXCLAIM2, new AddMessageBolt("2!", "2!!"), 5).shuffleGrouping(SPLITTER, STREAM2);
        builder.setBolt(PLUS2, new AddMessageBolt("2+", "2++"), 3).shuffleGrouping(EXCLAIM2);

        return builder;
    }

    public static void main(String[] args) throws Exception {

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(3);

        StormSubmitter.submitTopologyWithProgressBar(args[0], conf, newTopology().createTopology());

    }
}
