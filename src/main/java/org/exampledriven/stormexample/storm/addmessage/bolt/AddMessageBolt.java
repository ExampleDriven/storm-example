package org.exampledriven.stormexample.storm.addmessage.bolt;

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

public class AddMessageBolt extends BaseBasicBolt {
    public static final String WORD = "word";
    public static final String WORD_2 = "word2";
    public static final String ID = "id";
    private final String message1;
    private final String message2;
    OutputCollector collector;

    public AddMessageBolt(String message1, String message2) {
        this.message1 = message1;
        this.message2 = message2;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String word = tuple.getStringByField(WORD);
        Object id = tuple.getValue(0);

        collector.emit(new Values(id, word + message1, word + message2));
        collector.emit(new Values(id, word + message2, word + message1));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(ID, WORD, WORD_2));
    }
}
