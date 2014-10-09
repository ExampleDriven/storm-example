package org.exampledriven.stormexample.storm.addmessage.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

public class FieldRenameBolt extends BaseRichBolt {
    private final String originalName;
    private final String newName;
    OutputCollector collector;

    public FieldRenameBolt(String originalName, String newName) {
        this.originalName = originalName;
        this.newName = newName;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField(originalName);
        Object id = tuple.getValue(0);

        collector.emit(tuple, new Values(id, word));

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", newName));
    }

}
