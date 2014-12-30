package com.hortonworks.tutorials.wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 *
 * @author Kumar Kandasami
 */
public class WordCountTopology 
{

    private static final String SENTENCE_SPROUT_ID = "sentence-spout";
    private static final String SPLIT_BOLT_ID = "split-bolt";
    private static final String COUNT_BOLT_ID = "count-bolt";
    private static final String REPORT_BOLT_ID = "report-bolt";
    private static final String TOPOLOGY_NAME = "word-count-topology";
    
    public static void  main(String[] str) throws Exception
    {
    
        SentenceSpout spout = new SentenceSpout();
        SplitSentenceBolt splitBolt = new SplitSentenceBolt();
        WordCountBolt countBolt = new WordCountBolt();
        ReportBolt reportBolt = new ReportBolt();
        
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout(SENTENCE_SPROUT_ID, spout);
        
        builder.setBolt(SPLIT_BOLT_ID, splitBolt)
                .shuffleGrouping(SENTENCE_SPROUT_ID);
        
        builder.setBolt(COUNT_BOLT_ID, countBolt)
                .fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
        
        builder.setBolt(REPORT_BOLT_ID, reportBolt)
                .globalGrouping(COUNT_BOLT_ID);
        
        Config config = new Config();
        
        if (str.length == 0) //Local Cluster
        {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        
            Thread.sleep(10000);
        
            cluster.killTopology(TOPOLOGY_NAME);
            cluster.shutdown();
        }
        else //Remote Cluster
        {
            StormSubmitter.submitTopology(str[0], config, builder.createTopology());
        }
       
        
        
    }
}
