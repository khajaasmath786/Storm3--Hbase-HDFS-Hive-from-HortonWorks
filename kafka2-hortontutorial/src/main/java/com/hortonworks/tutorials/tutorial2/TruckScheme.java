package com.hortonworks.tutorials.tutorial2;

import java.io.UnsupportedEncodingException;
import java.sql.Timestamp;
import java.util.List;

import org.apache.log4j.Logger;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TruckScheme implements Scheme 
{

        public static final String FIELD_DRIVER_ID  = "driverId";
        public static final String FIELD_TRUCK_ID   = "truckId";
        public static final String FIELD_EVENT_TIME = "eventTime";
        public static final String FIELD_EVENT_TYPE = "eventType";
        public static final String FIELD_LONGITUDE  = "longitude";
        public static final String FIELD_LATITUDE   = "latitude";
        
        public static final String FIELD_INCIDENT_CNT = "incidentCnt";
        
        
	private static final long serialVersionUID = -2990121166902741545L;

	private static final Logger LOG = Logger.getLogger(TruckScheme.class);
	
        /**
         * <timestamp>|truckid|driverId|eventType|long|lat
         * @param bytes
         * @return 
         */

	public List<Object> deserialize(byte[] bytes)
        {
		try 
                {
			String truckEvent = new String(bytes, "UTF-8");
			String[] pieces = truckEvent.split("\\|");
			
			Timestamp eventTime = Timestamp.valueOf(pieces[0]);
			String truckId = pieces[1];
			String driverId = pieces[2];
			String eventType = pieces[3];
			String longitude= pieces[4];
			String latitude  = pieces[5];
			return new Values(cleanup(driverId), cleanup(truckId), 
                                    eventTime, cleanup(eventType), cleanup(longitude), cleanup(latitude));
			
		} 
                catch (UnsupportedEncodingException e) 
                {
                    LOG.error(e);
                    throw new RuntimeException(e);
		}
		
	}
        

	public Fields getOutputFields()
        {
            return new Fields(FIELD_DRIVER_ID,
                              FIELD_TRUCK_ID, 
                             FIELD_EVENT_TIME, 
                             FIELD_EVENT_TYPE, 
                             FIELD_LONGITUDE, 
                             FIELD_LATITUDE);
		
	}
        
        private String cleanup(String str)
        {
            if (str != null)
            {
                return str.trim().replace("\n", "").replace("\t", "");
            } 
            else
            {
                return str;
            }
            
        }
}