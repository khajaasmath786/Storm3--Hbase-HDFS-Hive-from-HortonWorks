package com.hortonworks.tutorials.wordcount;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;

/**
 *
 * @author Kumar Kandasami
 */
public class SentenceSpout  extends BaseRichSpout{

    private String[] sentences = {
        "Sentence1 Hello World",
        "Sentence2 Hello HortonWorks",
        "Sentence3 Hello Hapood"
    };
    private SpoutOutputCollector collector;
    private int index = 0;
    
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) 
    {
        declarer.declare(new Fields("sentence"));
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) 
    {
        this.collector = collector;
    }

    public void nextTuple() 
    {
        
            this.collector.emit(new Values(sentences[index]));
            index ++;
            if (index >= sentences.length) index = 0;
            Utils.sleep(1000);
        
    }
    
}
