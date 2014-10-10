package org.exampledriven.stormexample.addmessage.bolt;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.gson.Gson;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MessageCollectorBolt extends BaseBatchBolt {
    private Object id;
    private BatchOutputCollector collector;

    private Set<String> result;

    public MessageCollectorBolt() {
    }

    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
        result = new HashSet<>();
        this.id = id;
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {

        result.add(tuple.getStringByField("word2"));
        result.add(tuple.getStringByField("word"));

    }

    @Override
    public void finishBatch() {
        String jsonResult = new Gson().toJson(result);
        collector.emit(new Values(id, jsonResult));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "result"));
    }
}
