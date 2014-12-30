package com.hortonworks.tutorials.tutorial4;


import java.sql.Timestamp;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TruckHBaseBolt implements IRichBolt 
{
    private static final long serialVersionUID = 2946379346389650318L;
    private static final Logger LOG = Logger.getLogger(TruckHBaseBolt.class);

    //TABLES
    private static final String EVENTS_TABLE_NAME =  "truck_events";
    private static final String EVENTS_COUNT_TABLE_NAME = "driver_dangerous_events";

    //CF
    private static final byte[] CF_EVENTS_TABLE = Bytes.toBytes("events");
    private static final byte[] CF_EVENTS_COUNT_TABLE = Bytes.toBytes("count");

    //COL
    private static final byte[] COL_COUNT_VALUE = Bytes.toBytes("value");

    private static final byte[] COL_DRIVER_ID = Bytes.toBytes("d");
    private static final byte[] COL_TRUCK_ID = Bytes.toBytes("t");
    private static final byte[] COL_EVENT_TIME = Bytes.toBytes("tim");
    private static final byte[] COL_EVENT_TYPE = Bytes.toBytes("e");
    private static final byte[] COL_LATITUDE = Bytes.toBytes("la");
    private static final byte[] COL_LONGITUDE = Bytes.toBytes("lo");


    private OutputCollector collector;
    private HConnection connection;
    private HTableInterface eventsCountTable;
    private HTableInterface eventsTable;

    public TruckHBaseBolt(Properties topologyConfig) 
    {

    }

    
    public void prepare(Map stormConf, TopologyContext context,
                    OutputCollector collector) 
    {
        this.collector = collector;
        try 
        {
            this.connection = HConnectionManager.createConnection(constructConfiguration());
            this.eventsCountTable = connection.getTable(EVENTS_COUNT_TABLE_NAME);	
            this.eventsTable = connection.getTable(EVENTS_TABLE_NAME);
        } 
        catch (Exception e) 
        {
            String errMsg = "Error retrievinging connection and access to HBase Tables";
            LOG.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }		
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

        long incidentTotalCount = getInfractionCountForDriver(driverId);

        try 
        {

            Put put = constructRow(EVENTS_TABLE_NAME, driverId, truckId, eventTime, eventType,
                                latitude, longitude);
            this.eventsTable.put(put);

        } 
        catch (Exception e) 
        {
            LOG.error("Error inserting event into HBase table["+EVENTS_TABLE_NAME+"]", e);
        }

        if(!eventType.equalsIgnoreCase("normal")) 
        {
            try 
            {
                //long incidentTotalCount = getInfractionCountForDriver(driverId);

                incidentTotalCount = this.eventsCountTable.incrementColumnValue(Bytes.toBytes(driverId), CF_EVENTS_COUNT_TABLE, 
                                                                                                        COL_COUNT_VALUE, 1L);
                LOG.info("Success inserting event into counts table");

            } 
            catch (Exception e)
            {
               LOG.error("Error inserting violation event into HBase table", e);
            }				
        } 
        collector.emit(tuple, new Values(driverId, truckId, eventTime, eventType, longitude, latitude, incidentTotalCount));
        //acknowledge even if there is an error
        collector.ack(tuple);
    }


    public static  Configuration constructConfiguration()
    {
        Configuration config = HBaseConfiguration.create();
        return config;
    }	


    private Put constructRow(String columnFamily, String driverId, String truckId, 
                Timestamp eventTime, String eventType, String latitude, String longitude) 
    {

        String rowKey = consructKey(driverId, truckId, eventTime);
        Put put = new Put(Bytes.toBytes(rowKey));

        put.add(CF_EVENTS_TABLE, COL_DRIVER_ID, Bytes.toBytes(driverId));
        put.add(CF_EVENTS_TABLE, COL_TRUCK_ID, Bytes.toBytes(truckId));

        long eventTimeValue=  eventTime.getTime();
        put.add(CF_EVENTS_TABLE, COL_EVENT_TIME, Bytes.toBytes(eventTimeValue));
        put.add(CF_EVENTS_TABLE, COL_EVENT_TYPE, Bytes.toBytes(eventType));
        put.add(CF_EVENTS_TABLE, COL_LATITUDE, Bytes.toBytes(latitude));
        put.add(CF_EVENTS_TABLE, COL_LONGITUDE, Bytes.toBytes(longitude));

        return put;
    }


    private String consructKey(String driverId, String truckId, Timestamp ts2)
    {
        long reverseTime = Long.MAX_VALUE - ts2.getTime();
        String rowKey = driverId+"|"+truckId+"|"+reverseTime;
        return rowKey;
    }	


    
    public void cleanup() 
    {
        try 
        {
                eventsCountTable.close();
                eventsTable.close();
                connection.close();
        } 
        catch (Exception  e) 
        {
                LOG.error("Error closing connections", e);
        }
    }

    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(TruckScheme.FIELD_DRIVER_ID, 
                    TruckScheme.FIELD_TRUCK_ID, TruckScheme.FIELD_EVENT_TIME, 
                    TruckScheme.FIELD_EVENT_TYPE, TruckScheme.FIELD_LONGITUDE, 
                    TruckScheme.FIELD_LATITUDE, TruckScheme.FIELD_INCIDENT_CNT));
    }

    
    public Map<String, Object> getComponentConfiguration()
    {
            return null;
    }

    private long getInfractionCountForDriver(String driverId)
    {

        try 
        {
            byte[] driver = Bytes.toBytes(driverId);
            Get get = new Get(driver);
            Result result = eventsCountTable.get(get);
            long count = 0;
            if(result != null) 
            {
                byte[] countBytes = result.getValue(CF_EVENTS_COUNT_TABLE, COL_COUNT_VALUE);
                if(countBytes != null) 
                {
                    count = Bytes.toLong(countBytes);
                }
            }
            return count;
        } 
        catch (Exception e) 
        {
            LOG.error("Error getting infraction count", e);
            throw new RuntimeException("Error getting infraction count");
        }
    }	
}
