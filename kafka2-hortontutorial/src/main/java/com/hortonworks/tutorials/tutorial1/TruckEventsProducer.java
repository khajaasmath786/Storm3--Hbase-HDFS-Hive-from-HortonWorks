package com.hortonworks.tutorials.tutorial1;

/**
 * TruckEventsProducer class simulates the real time truck event generation.
 *
 */
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class TruckEventsProducer {

    private static final Logger LOG = Logger.getLogger(TruckEventsProducer.class);

        public static void main(String[] args) 
	            throws ParserConfigurationException, SAXException, IOException, URISyntaxException 
	    {
	        if (args.length != 2) 
	        {
	            
	            System.out.println("Usage: TruckEventsProducer <broker list> <zookeeper>");
	            //localhost:9092 localhost:2181
	            System.exit(-1);
	        }
	        
	        LOG.debug("Using broker list:" + args[0] +", zk conn:" + args[1]);
	        
	        // long events = Long.parseLong(args[0]);
	        Random rnd = new Random();
	        
	        Properties props = new Properties();
	        props.put("metadata.broker.list", args[0]);
	        props.put("zk.connect", args[1]);
	        props.put("serializer.class", "kafka.serializer.StringEncoder");
	        props.put("request.required.acks", "1");
	
	        String TOPIC = "truckevent";
	        ProducerConfig config = new ProducerConfig(props);
	
	        Producer<String, String> producer = new Producer<String, String>(config);
	
	        String[] events = {"Normal", "Normal", "Normal", "Normal", "Normal", "Normal", "Lane Departure", 
	                           "Overspeed", "Normal", "Normal", "Normal", "Normal", "Lane Departure","Normal", 
	                           "Normal", "Normal", "Normal",  "Unsafe tail distance", "Normal", "Normal", 
	                           "Unsafe following distance", "Normal", "Normal", "Normal","Normal","Normal","Normal",
	                           "Normal","Normal","Normal","Normal","Normal","Normal","Normal", "Normal", "Overspeed"
	                            ,"Normal", "Normal","Normal","Normal","Normal","Normal","Normal" };
	        
	        Random random = new Random();
	
	        String finalEvent = "";
	        
	        // Get Latitude and Longitude Array List.        
	        String route17 = "route17.kml";
	        String[] arrayroute17 = GetKmlLanLangList(route17);
	        String route17k = "route17k.kml";
	        String[] arrayroute17k = GetKmlLanLangList(route17k);
	        String route208 = "route208.kml";
	        String[] arrayroute208 = GetKmlLanLangList(route208);
	        String route27 = "route27.kml";
	        String[] arrayroute27 = GetKmlLanLangList(route27);
	        
	        String[] truckIds = {"1", "2", "3","4"};
	        String[] routeName = {"route17", "route17k", "route208", "route27"};
	        String[] driverIds = {"11", "12", "13", "14"};
	        
	        int evtCnt = events.length;
	        
	        //Find max route arraysize.
	        int maxarraysize = arrayroute17.length;
	        if(maxarraysize < arrayroute17k.length)
	        	maxarraysize = arrayroute17k.length;
	        if(maxarraysize < arrayroute208.length)
	        	maxarraysize = arrayroute208.length;
	        if(maxarraysize < arrayroute27.length)
	        	maxarraysize = arrayroute208.length;
	        
	        for (int i = 0; i < maxarraysize; i++) 
	        {
	        	
	        	if(arrayroute17.length > i)
	        	{
		        	finalEvent = new Timestamp(new Date().getTime()) + "|"
		                    + truckIds[0] + "|" + driverIds[0] + "|" + events[random.nextInt(evtCnt)] 
		                    + "|" + getLatLong(arrayroute17[i]);
		        	try {
		                KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC, finalEvent);
		                LOG.info("Sending Messge #: " + routeName[0] + ": " + i +", msg:" + finalEvent);
		                producer.send(data);
		                Thread.sleep(1000);
		            } catch (Exception e) {
		                e.printStackTrace();
		            }
	        	}
	        	if(arrayroute17k.length > i)
	        	{
		        	finalEvent = new Timestamp(new Date().getTime()) + "|"
		                    + truckIds[1] + "|" + driverIds[1] + "|" + events[random.nextInt(evtCnt)] 
		                    + "|" + getLatLong(arrayroute17k[i]);
		        	try {
		                KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC, finalEvent);
		                LOG.info("Sending Messge #: " + routeName[1] + ": " + i +", msg:" + finalEvent);
		                producer.send(data);
		                Thread.sleep(1000);
		            } catch (Exception e) {
		                e.printStackTrace();
		            }
	        	}
	        	if(arrayroute208.length > i)
	        	{
		        	finalEvent = new Timestamp(new Date().getTime()) + "|"
		                    + truckIds[2] + "|" + driverIds[2] + "|" + events[random.nextInt(evtCnt)] 
		                    + "|" + getLatLong(arrayroute208[i]);
		        	try {
		                KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC, finalEvent);
		                LOG.info("Sending Messge #: " + routeName[2] + ": " + i +", msg:" + finalEvent);
		                producer.send(data);
		                Thread.sleep(1000);
		            } catch (Exception e) {
		                e.printStackTrace();
		            }
	        	}
	        	if(arrayroute27.length > i)
	        	{
		        	finalEvent = new Timestamp(new Date().getTime()) + "|"
		                    + truckIds[3] + "|" + driverIds[3] + "|" + events[random.nextInt(evtCnt)] 
		                    + "|" + getLatLong(arrayroute27[i]);
		        	try {
		                KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC, finalEvent);
		                LOG.info("Sending Messge #: " + routeName[3] + ": " + i +", msg:" + finalEvent);
		                producer.send(data);
		                Thread.sleep(1000);
		            } catch (Exception e) {
		                e.printStackTrace();
		            }
	        	}
	        }
	        
	//        for (int i = 0; i < arrayroute17.length; i++) 
	//        {
	//        	finalEvent = new Timestamp(new Date().getTime()) + "|"
	//                    + truckIds[0] + "|" + driverIds[0] + "|" + events[random.nextInt(evtCnt)] 
	//                    + "|" + getLatLong(arrayroute17[i]);
	//        	try {
	//                KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC, finalEvent);
	//                LOG.info("Sending Messge #: " + routeName[0] + ": " + i +", msg:" + finalEvent);
	//                producer.send(data);
	//                Thread.sleep(3000);
	//            } catch (Exception e) {
	//                e.printStackTrace();
	//            }
	//        }
	//        for (int j = 0; j < arrayroute17k.length; j++) 
	//        {
	//        	finalEvent = new Timestamp(new Date().getTime()) + "|"
	//        			+ truckIds[1] + "|" + driverIds[1] + "|" + events[random.nextInt(evtCnt)] 
	//                    + "|" + getLatLong(arrayroute17k[j]);
	//        	try {
	//                KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC, finalEvent);
	//                LOG.info("Sending Messge #: " + routeName[1] + ": " + j +", msg:" + finalEvent);
	//                producer.send(data);
	//                Thread.sleep(3000);
	//            } catch (Exception e) {
	//                e.printStackTrace();
	//            }
	//        }
	//
	//        for (int k = 0; k < arrayroute208.length; k++) 
	//        {
	//        	finalEvent = new Timestamp(new Date().getTime()) + "|" 
	//                    + truckIds[2] + "|" + driverIds[2] + "|" + events[random.nextInt(evtCnt)] 
	//                    + "|" + getLatLong(arrayroute208[k]);
	//        	try {
	//                KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC, finalEvent);
	//                LOG.info("Sending Messge #: " + routeName[2] + ": " + k +", msg:" + finalEvent);
	//                producer.send(data);
	//                Thread.sleep(3000);
	//            } catch (Exception e) {
	//                e.printStackTrace();
	//            }
	//        }
	//        
	//        for (int l = 0; l < arrayroute27.length; l++) 
	//        {
	//        	finalEvent = new Timestamp(new Date().getTime()) + "|" 
	//                    + truckIds[3] + "|" + driverIds[3] + "|" + events[random.nextInt(evtCnt)] 
	//                    + "|" + getLatLong(arrayroute27[l]);
	//        	try {
	//                KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC, finalEvent);
	//                LOG.info("Sending Messge #: " + routeName[3] + ": " + l +", msg:" + finalEvent);
	//                producer.send(data);
	//                Thread.sleep(3000);
	//            } catch (Exception e) {
	//                e.printStackTrace();
	//            }
	//        }
	        
	        producer.close();
	    }

    private static String getLatLong(String str)
    {
        str=str.replace("\t", "");
        str=str.replace("\n", "");
        
        String[] latLong = str.split("|");
        
        if (latLong.length == 2)
        {
            return latLong[1].trim() + "|" + latLong[0].trim();
        } 
        else 
        {
            return str;    
        }
    }
    
	public static String[] GetKmlLanLangList(String urlString) throws ParserConfigurationException, SAXException, IOException {
        String[] array = null;
        String[] array2 = null;
        
        Document doc = null;
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        doc = db.parse(ClassLoader.getSystemResourceAsStream(urlString));
        doc.getDocumentElement().normalize();
        NodeList nList = doc.getElementsByTagName("LineString");
        System.out.println(nList.getLength());
        for (int temp = 0; temp < nList.getLength();temp++) {
        	
                Node nNode = nList.item(temp);
                if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element eElement = (Element) nNode;
                    String strLatLon = eElement.getElementsByTagName("coordinates").item(0).getTextContent().toString();
                    array = strLatLon.split(" ");
                    array2 = new String[array.length];
                    for(int i = 0; i< array.length; i++)
                    {
                    	array2[i]=array[i].replace(',' ,'|');
                    }
                                        
                }
            
        }
        return array2;
    }
}
