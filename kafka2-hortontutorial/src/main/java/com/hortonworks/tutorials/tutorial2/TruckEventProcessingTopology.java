package com.hortonworks.tutorials.tutorial2;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

/**
 *
 * @author Kumar Kandasami
 */
public class TruckEventProcessingTopology extends BaseTruckEventTopology 
{
    private static final String KAFKA_SPOUT_ID = "kafkaSpout"; 
    private static final String LOG_TRUCK_BOLT_ID = "logTruckEventBolt";
            
    public TruckEventProcessingTopology(String configFileLocation) throws Exception 
    {
        super(configFileLocation);
    }

    private SpoutConfig constructKafkaSpoutConf() 
    {
        BrokerHosts hosts = new ZkHosts(topologyConfig.getProperty("kafka.zookeeper.host.port"));
        String topic = topologyConfig.getProperty("kafka.topic");
        String zkRoot = topologyConfig.getProperty("kafka.zkRoot");
        String consumerGroupId = "StormSpout";

        SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, consumerGroupId);

        /* Custom TruckScheme that will take Kafka message of single truckEvent 
         * and emit a 2-tuple consisting of truckId and truckEvent. This driverId
         * is required to do a fieldsSorting so that all driver events are sent to the set of bolts */
        spoutConfig.scheme = new SchemeAsMultiScheme(new TruckScheme());

        return spoutConfig;
    }
    
    
    
    public void configureKafkaSpout(TopologyBuilder builder) 
    {
        KafkaSpout kafkaSpout = new KafkaSpout(constructKafkaSpoutConf());
        int spoutCount = Integer.valueOf(topologyConfig.getProperty("spout.thread.count"));
        builder.setSpout(KAFKA_SPOUT_ID, kafkaSpout);
    }
    
    public void configureLogTruckEventBolt(TopologyBuilder builder)
    {
        LogTruckEventsBolt logBolt = new LogTruckEventsBolt();
        builder.setBolt(LOG_TRUCK_BOLT_ID, logBolt).globalGrouping(KAFKA_SPOUT_ID);
    }
    
    private void buildAndSubmit() throws Exception
    {
    	//home/edureka/Lunoworkspace/kafka2-hortontutorial/target/kafka2-hortontutorial-0.0.1-SNAPSHOT.jar
        TopologyBuilder builder = new TopologyBuilder();
        //sudo bin/storm jar /home/edureka/Lunoworkspace/kafka2-hortontutorial/target/kafka2-hortontutorial-0.0.1-SNAPSHOT.jar com.hortonworks.tutorials.tutorial2.TruckEventProcessingTopology

        configureKafkaSpout(builder);
        configureLogTruckEventBolt(builder);
        
        Config conf = new Config();
	conf.setDebug(true);
        
        StormSubmitter.submitTopology("truck-event-processor", 
                                    conf, builder.createTopology());
    }

    public static void main(String[] str) throws Exception
    {
        // Load Properties file from the classpath.
    	String configFileLocation = "truck_event_topology.properties";
        
        TruckEventProcessingTopology truckTopology 
                = new TruckEventProcessingTopology(configFileLocation);
        truckTopology.buildAndSubmit();
    }

}
