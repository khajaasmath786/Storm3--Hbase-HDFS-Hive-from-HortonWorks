package com.hortonworks.tutorials.wordcount;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Kumar Kandasami
 */
public class ReportBolt extends BaseRichBolt
{

    private HashMap<String, Long> counts = null;
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) 
    {
        //no field output.
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
    {
        this.counts = new HashMap<String, Long>();
    }

    public void execute(Tuple input) 
    {
    
        String word = input.getStringByField("word");
        Long count = input.getLongByField("count");
        this.counts.put(word, count);
    }
    
    public void cleanup() 
    {
        System.out.println("------- Counts -------");
        ArrayList<String> l = new ArrayList<String>(counts.keySet());
        Collections.sort(l);
        for (String key: l)
        {
            System.out.println(key + "," + counts.get(key));
        }
    }

    
}
