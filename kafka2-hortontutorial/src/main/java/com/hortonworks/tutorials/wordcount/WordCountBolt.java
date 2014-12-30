package com.hortonworks.tutorials.wordcount;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Kumar Kandasami
 */
public class WordCountBolt extends BaseRichBolt
{
    private OutputCollector collector;
    private HashMap<String, Long> counts = null;

    public void declareOutputFields(OutputFieldsDeclarer declarer) 
    {
        declarer.declare(new Fields("word","count"));
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
    {
        this.collector = collector;
        this.counts = new HashMap<String, Long>();
    }

    public void execute(Tuple input) 
    {
        String word = input.getStringByField("word");
        Long count = this.counts.get(word);
        if (count == null) count = 0L;
        count ++;
        this.counts.put(word, count);
        this.collector.emit(new Values(word, count));
    }
    
}
