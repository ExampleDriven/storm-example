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

import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.tuple.Fields;
import org.exampledriven.stormexample.base.DrpcTopologyBuilder;
import org.exampledriven.stormexample.addmessage.bolt.AddMessageBolt;
import org.exampledriven.stormexample.addmessage.bolt.MessageCollectorBolt;
import org.exampledriven.stormexample.addmessage.bolt.FieldRenameBolt;

public class AddMessageLinearDRPCTopology extends DrpcTopologyBuilder {

    protected LinearDRPCTopologyBuilder getLinearDRPCTopologyBuilder() {

        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder(getHandlerName());

        builder.addBolt(new FieldRenameBolt("args", "word"), 5).shuffleGrouping();
        builder.addBolt(new AddMessageBolt("!", "!!"), 5).shuffleGrouping();
        builder.addBolt(new AddMessageBolt("+", "++"), 3).shuffleGrouping();
        builder.addBolt(new MessageCollectorBolt(), 3).fieldsGrouping(new Fields("id"));

        return builder;
    }

    @Override
    public String getHandlerName() {
        return "exclamation";
    }

}
