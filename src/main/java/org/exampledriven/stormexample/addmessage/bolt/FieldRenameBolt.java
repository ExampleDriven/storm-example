package org.exampledriven.stormexample.addmessage.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

public class FieldRenameBolt extends BaseBasicBolt {
    private final String originalName;
    private final String newName;
    OutputCollector collector;

    public FieldRenameBolt(String originalName, String newName) {
        this.originalName = originalName;
        this.newName = newName;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", newName));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String word = tuple.getStringByField(originalName);
        Object id = tuple.getValue(0);

        collector.emit(new Values(id, word));


    }
}
