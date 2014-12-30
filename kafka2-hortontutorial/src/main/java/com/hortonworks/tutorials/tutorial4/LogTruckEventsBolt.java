package com.hortonworks.tutorials.tutorial4;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import java.util.Map;
import org.apache.log4j.Logger;

/**
 *
 * @author Kumar Kandasami
 */
public class LogTruckEventsBolt extends BaseRichBolt
{
    private static final Logger LOG = Logger.getLogger(LogTruckEventsBolt.class);
    
    public void declareOutputFields(OutputFieldsDeclarer ofd) 
    {
       //none prints to the Logger.
    }

    public void prepare(Map map, TopologyContext tc, OutputCollector oc) 
    {
       //no output.
    }

    public void execute(Tuple tuple) 
    {
      LOG.info(tuple.getStringByField(TruckScheme.FIELD_DRIVER_ID)  + "," + 
              tuple.getStringByField(TruckScheme.FIELD_TRUCK_ID)    + "," +
              tuple.getValueByField(TruckScheme.FIELD_EVENT_TIME)  + "," +
              tuple.getStringByField(TruckScheme.FIELD_EVENT_TYPE)  + "," +
              tuple.getStringByField(TruckScheme.FIELD_LATITUDE)    + "," +
              tuple.getStringByField(TruckScheme.FIELD_LONGITUDE));
    }
    
}
