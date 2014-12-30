package com.hortonworks.tutorials.tutorial4;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.log4j.Logger;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;

public class OverSpeedMonitorBolt implements IRichBolt 
{
    private static final long serialVersionUID = 2946379346389650318L;
    private static final Logger LOG = Logger.getLogger(OverSpeedMonitorBolt.class);

    private Twitter twitter;
    private Map<String, String> driverInfo;
    

    public OverSpeedMonitorBolt(Properties topologyConfig) 
    {

    }

    
    public void prepare(Map stormConf, TopologyContext context,
                    OutputCollector collector) 
    {
      //  this.collector = collector;
        this.twitter = new TwitterFactory().getInstance();
        
        this.driverInfo = new HashMap<String, String>();
        driverInfo.put ("11", "Jim C");
        driverInfo.put ("12", "Jackie K");
        driverInfo.put ("13",  "Kevin B");
        
    }

    
    public void execute(Tuple tuple)
    {

        //LOG.info("About to insert tuple["+input +"] into HBase...");

        String driverId = tuple.getStringByField(TruckScheme.FIELD_DRIVER_ID);
        String truckId = tuple.getStringByField(TruckScheme.FIELD_TRUCK_ID);
        Timestamp eventTime = (Timestamp) tuple.getValueByField(TruckScheme.FIELD_EVENT_TIME);
        String eventType = tuple.getStringByField(TruckScheme.FIELD_EVENT_TYPE);
        String longitude = tuple.getStringByField(TruckScheme.FIELD_LONGITUDE);
        String latitude = tuple.getStringByField(TruckScheme.FIELD_LATITUDE);

        if ("overspeed".equals(eventType.trim().toLowerCase()))
        {
            LOG.info("Detected an overspeed event .. sending tweet");
            
            try
            {
                StringBuilder msg = new StringBuilder(driverInfo.get(driverId));
                msg.append(" over speeding at ").append(eventTime.toString());
                msg.append(" ").append(latitude).append("/").append(longitude);
                
                this.twitter.updateStatus(msg.toString());
                
            }
            catch (Throwable t)
            {
                t.printStackTrace();
                LOG.error("Error Sending tweet on overspeed event.", t);
            }
        }
        
    }

    public void cleanup() 
    {
    }

    public void declareOutputFields(OutputFieldsDeclarer ofd) 
    {
    }

    public Map<String, Object> getComponentConfiguration() 
    {
        return null;
    }


}
