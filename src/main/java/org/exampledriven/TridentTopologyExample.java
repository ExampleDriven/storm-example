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
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;

import java.util.Map;

/**
 * This is a basic example of a Storm topology.
 */
public class TridentTopologyExample {

    public static final String WORD_SPOUT = "word spout";
    public static final String EXCLAIM = "exclaim";
    public static final String HYPHEN = "hyphen";

    public static class MessageBolt extends BaseRichBolt {
        private final String message;
        OutputCollector _collector;

        public MessageBolt(String message) {
            this.message = message;
        }

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            _collector.emit(tuple, new Values(tuple.getStringByField("word") + message));
            _collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

    }

    public static void main(String[] args) throws Exception {

        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"), new Values("how many apples can you eat"));
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();
        TridentState wordCounts = topology.newStream("spout1", spout)
                .each(new Fields("sentence"), new TridentWordCount.Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count")).parallelismHint(6);

        //        DRPCClient client = new DRPCClient("drpc.server.location", 3772);
        //        // prints the JSON-encoded result, e.g.: "[[5078]]"
        //        System.out.println(client.execute("words", "cat dog the man"));

        Stream aggregate = topology.newDRPCStream("words")
                .each(new Fields("args"), new TridentWordCount.Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
                .each(new Fields("count"), new FilterNull())
                .aggregate(new Fields("count"), new Sum(), new Fields("sum"));

        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, topology.build());
        } else {

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, topology.build());
            Utils.sleep(10000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }

//    public void test() {
//
//        TridentTopology topology = new TridentTopology();
//
//        TridentState urlToTweeters = topology.newStaticState(getUrlToTweetersState());
//        TridentState tweetersToFollowers = topology.newStaticState(getTweeterToFollowersState());
//
//        topology.newDRPCStream("reach")
//                .stateQuery(urlToTweeters, new Fields("args"), new MapGet(), new Fields("tweeters"))
//                .each(new Fields("tweeters"), new TridentReach.ExpandList(), new Fields("tweeter")).shuffle()
//                .stateQuery(tweetersToFollowers, new Fields("tweeter"), new MapGet(), new Fields("followers"))
//                .parallelismHint(200).each(new Fields("followers"), new TridentReach.ExpandList(), new Fields("follower"))
//                .groupBy(new Fields("follower")).aggregate(new TridentReach.One(), new Fields("one")).parallelismHint(20)
//                .aggregate(new Count(), new Fields("reach"));
//    }
}
