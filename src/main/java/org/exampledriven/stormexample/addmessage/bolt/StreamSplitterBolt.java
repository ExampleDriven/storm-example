package org.exampledriven.stormexample.addmessage.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class StreamSplitterBolt extends BaseBasicBolt {
    public static final String WORD = "word";
    public static final String ID = "id";
    private final String stream1;
    private final String stream2;
    OutputCollector collector;

    public StreamSplitterBolt(String stream1, String stream2) {
        this.stream1 = stream1;
        this.stream2 = stream2;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String word = tuple.getStringByField(WORD);
        Object id = tuple.getValue(0);

        collector.emit(stream1, new Values(id, word));
        collector.emit(stream2, new Values(id, word));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(stream1,new Fields(ID, WORD));
        declarer.declareStream(stream2,new Fields(ID, WORD));
    }
}
