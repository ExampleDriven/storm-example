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
package org.exampledriven.stormexample.storm.reach;

import backtype.storm.Config;
import backtype.storm.ILocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.*;
import storm.trident.operation.builtin.Count;
import storm.trident.tuple.TridentTuple;

import java.util.*;

public class ReachTridentTopology {

    public static final String TWEETER = "tweeter";
    public static final String FOLLOWER = "follower";
    public static final String HANDLER_NAME = "reach-trident";

    public static Map<String, List<String>> TWEETERS_DB = new HashMap<String, List<String>>() {{
        put("foo.com/blog/1", Arrays.asList("sally", "bob", "tim", "george", "nathan"));
        put("engineering.twitter.com/blog/5", Arrays.asList("adam", "david", "sally", "nathan"));
        put("tech.backtype.com/blog/123", Arrays.asList("tim", "mike", "john"));
        put("example.com", Arrays.asList("peter", "rebecca"));
    }};

    public static Map<String, List<String>> FOLLOWERS_DB = new HashMap<String, List<String>>() {{
        put("sally", Arrays.asList("bob", "tim", "alice", "adam", "jim", "chris", "jai"));
        put("bob", Arrays.asList("sally", "nathan", "jim", "mary", "david", "vivian"));
        put("tim", Arrays.asList("alex"));
        put("nathan", Arrays.asList("sally", "bob", "adam", "harry", "chris", "vivian", "emily", "jordan"));
        put("adam", Arrays.asList("david", "carissa"));
        put("mike", Arrays.asList("john", "bob"));
        put("john", Arrays.asList("alice", "nathan", "jim", "mike", "bob"));
        put("peter", Arrays.asList("rebecca", "rachel"));
        put("rebecca", Arrays.asList("rachel"));
    }};

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

        stream
            .each(new Fields("args"), new GetTweeters(), new Fields(TWEETER))
            .each(new Fields(TWEETER), new GetFollowers(), new Fields(FOLLOWER))
            .groupBy(new Fields(FOLLOWER))
            .aggregate(new Count(), new Fields("unique follower"))
            .aggregate(new Count(), new Fields("reach"));

    }

    public static class GetTweeters extends BaseFunction {

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String url = tuple.getString(0);
            List<String> tweeters = TWEETERS_DB.get(url);
            if (tweeters != null) {
                for (String tweeter : tweeters) {
                    collector.emit(new Values(tweeter));
                }
            }
        }

    }

    public static class GetFollowers extends BaseFunction {

        @Override public void execute(TridentTuple tuple, TridentCollector collector) {
            String tweeter = tuple.getStringByField(TWEETER);
            List<String> followers = FOLLOWERS_DB.get(tweeter);
            if (followers != null) {
                for (String follower : followers) {
                    collector.emit(new Values(follower));
                }
            }
        }
    }

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        Config conf = new Config();
        conf.setDebug(true);
        StormSubmitter.submitTopologyWithProgressBar("ReachTridentTopology", conf,
                ReachTridentTopology.newRemoteDRPCTridentTopology().build());
    }

}
