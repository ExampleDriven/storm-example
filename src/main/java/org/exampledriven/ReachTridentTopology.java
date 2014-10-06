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
import storm.trident.operation.*;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Debug;
import storm.trident.operation.builtin.MapGet;
import storm.trident.tuple.TridentTuple;

import java.util.*;

/**
 * This is a good example of doing complex Distributed RPC on top of Storm. This program creates a topology that can
 * compute the reach for any URL on Twitter in realtime by parallelizing the whole computation.
 * <p/>
 * Reach is the number of unique people exposed to a URL on Twitter. To compute reach, you have to get all the people
 * who tweeted the URL, get all the followers of all those people, unique that set of followers, and then count the
 * unique set. It's an intense computation that can involve thousands of database calls and tens of millions of follower
 * records.
 * <p/>
 * This Storm topology does every piece of that computation in parallel, turning what would be a computation that takes
 * minutes on a single machine into one that takes just a couple seconds.
 * <p/>
 * For the purposes of demonstration, this topology replaces the use of actual DBs with in-memory hashmaps.
 * <p/>
 * See https://github.com/nathanmarz/storm/wiki/Distributed-RPC for more information on Distributed RPC.
 */
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
        //        stream.each(new Fields("args"), new GetTweeters(), new Fields(TWEETER))
        //            .each(new Fields(TWEETER), new GetFollowers(), new Fields(FOLLOWER))
        //            .each(new Fields(FOLLOWER), new UniqueFilter())
        //            .aggregate(new Count(), new Fields("reach"));

        stream.each(new Fields("args"), new Debug("input"))
                .each(new Fields("args"), new GetTweeters(), new Fields(TWEETER))
                .each(new Fields(TWEETER), new Debug("GetTweeters"))

                .each(new Fields(TWEETER), new GetFollowers(), new Fields(FOLLOWER))
                .each(new Fields(FOLLOWER), new Debug("-GetFollowers"))

                .each(new Fields(FOLLOWER), new UniqueFilter())
                .each(new Fields(FOLLOWER), new Debug("--UniqueFilter"))

                .aggregate(new Fields(FOLLOWER), new Count(), new Fields("reach"))
                .each(new Fields("reach"), new Debug("---count"));

//        topology.newDRPCStream("reach")
//                .stateQuery(urlToTweeters, new Fields("args"), new MapGet(), new Fields("tweeters"))
//                .each(new Fields("tweeters"), new ExpandList(), new Fields("tweeter")).shuffle()
//                .stateQuery(tweetersToFollowers, new Fields("tweeter"), new MapGet(), new Fields("followers"))
//                .parallelismHint(200).each(new Fields("followers"), new ExpandList(), new Fields("follower"))
//                .groupBy(new Fields("follower")).aggregate(new One(), new Fields("one")).parallelismHint(20)
//                .aggregate(new Count(), new Fields("reach"));

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
