package com.hortonworks.tutorials.wordcount;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Map;

/**
 *
 * @author Kumar Kandasami
 */
public class SplitSentenceBolt extends BaseRichBolt{

    private OutputCollector collector;
    public void declareOutputFields(OutputFieldsDeclarer declarer) 
    {
        declarer.declare(new Fields("word"));
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) 
    {
        this.collector = collector;
    }

    public void execute(Tuple input) 
    {
        String sentence = input.getStringByField("sentence");
        String[] words = sentence.split(" ");
        for (String word: words) {
            this.collector.emit(new Values(word));
        }
    }
    
}
