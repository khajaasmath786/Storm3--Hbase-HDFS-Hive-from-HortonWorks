Storm3--Hbase-HDFS-Hive-from-HortonWorks
========================================

Storm3- Hbase HDFS Hive from HortonWorks

===================== Reference from Horton Works Tutorial 

http://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.1.5/bk_user-guide/content/ch_storm-using-hbase-connector.html

==================== Storm and Hadoop Eco systems versions  ================

1.Storm - 0.9.3

2.Kafka - 0.8.x

3.Hbase - 0.96.2-hadoop2

4.Hadoop - 2.2.0 

5.hive -  0.13.1

You can always find the versions from the conf/lib jar files . Jar files has the version numbers.
Also see pom.xml to see the versions used in this project.

======================================== My Notes ========================================

Storm Kafka Integration and loading data into hbase,hdfs and hive.

===================== Pre-SetUp

Always remove the data directory  and logs of storm,kafka and zoo keeper before working .

1. sudo rm -r /hbase/hbasestorage/zookeeper/version-2 --> folders inside it, setting of zoo by hbase
2. sudo rm -r /hbase/hbasestorage/zookeeper/myid --> folders inside it, setting of zoo by hbase
3. sudo rm -r /storm/logs
4. sudo rm -r /tmp/kafka-logs
5. sudo rm -r /tmp/zoo   ---> This is setting of kafka for zoo keeper available in kafka/conf/zookeeper.properties

====================Must Read Information

1. Version should be same while dealing with kafkaspout of storm and storm core.

If tuples are not received to bolts and spouts, make sure version is correct. I had problem with versions so installed new version 0.9.3.

2. Storm UI was stuck at loading page so modified port number from storm.yaml file and changed to 8773

3. Start hbase and hive before submitting tuples. Also make sure the version in pom.xml is equal to the one available in hadoop cluster.
you can get versions of hbase and hive by navigating to lib folder of hive and hbase installation and see the jar files with version number

4. start hive metastore with  command hive --service metastore

5. If hive metastore is throwing error while executing hive command, then update hive-site.xml in conf folder of hive with below property.

6. start hive metastore with the command 
hive --service metastore   (no sudo here)

7. To run the Hive from the client first you have to start the thrift server
Command to Start the thrift server : HIVE_PORT=10000 sudo /usr/lib/hive/bin/hive --service hiveserver
Note: See ticket raised in edureka. 

8. Use JDBC program provided by Karan to execute using thrift server.
9. Always remove the data directory  and logs of storm,kafka and zoo keeper before working .
sudo rm -r /hbase/hbasestorage/zookeeper/version-2 --> folders inside it, setting of zoo by hbase
sudo rm -r /hbase/hbasestorage/zookeeper/myid --> folders inside it, setting of zoo by hbase
sudo rm -r /storm/logs
sudo rm -r /tmp/kafka-logs
sudo rm -r /tmp/zoo   ---> This is setting of kafka for zoo keeper
9. If you are not able to start the supervisor node. This setting was mentioned in storm.yaml file.
cleanup the data folder from /home/user/storm/data using command sudo rm -r /home/user/storm/data
10. Worker logs are present in storm installation folder . /usr/lib/storm/logs.
11. If you are not able to start the supervisor node. This setting was mentioned in storm.yaml file.
cleanup the data folder from /home/user/storm/data using command sudo rm -r /home/user/storm/data

12. Worker logs are present in storm installation folder . /usr/lib/storm/logs. See this logs to trouble shoot the spouts and bolts.

13. bin/storm kill topologyname .You can do this from stormUI also.

14. Follow all 5 steps till consume in my kafka github.

sudo bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic truckevent --from-beginning

===================== Starting hbase,hive,kafka and storm ===================== 

1. POm.xml should have correct versions

2. hbase-site.xml should be included in class path and the project should be build with dependencies. This was mentioned in many forums.

3. Generate jar file with pom.xml.

4. Start Hbase. Follow Edureka readme file on how to start.
cd /usr/lib/hbase-0.96.2-hadoop2/
./bin/start-hbase.sh
hbase shell

5. I am using pseduo mode so seperate Zoo Keeper installation is not required as Hbase internally has it. No need to start hbase explicitly

6. sudo jps . Make sure hmaster,hregion,hquorumpeer are listed as java process.

7. Start Kafka only if zoo keeper is up. That is hquorumpeer is listed in jps command.

8. Start Hive Meta store using the command $hive --service metastore . If any errors are encountered you need to update hive-site.xml to enable local mode. hive.metastore.local=true should be set in hive-site.xml

9. Start storm. Navigate to storm folder and start with below commands. Dont close terminals after storm is started.
cd usr/lib/storm  then execute sudo bin/storm nimbus.
cd usr/lib/storm  then execute sudo bin/storm supervisor.
cd usr/lib/storm  then execute sudo bin/storm ui.

if supervisor or nimbus terminals are interuppted , delete the storm logs and data directory. sudo rm -r /home/user/storm/data that is defined in storm.yaml file.

===================== Submitting Topology to Storm Cluster ==================================

Upload the topology to storm cluster with below commands

10. Navigate to storm installation folder(cd usr/lib/storm). Submit Topology with below command

 sudo bin/storm jar /home/edureka/Lunoworkspace/kafka2-hortontutorial/target/kafka2-hortontutorial-0.0.1-SNAPSHOT.jar com.hortonworks.tutorials.tutorial3.TruckEventProcessingTopology

11. If Topology already exists, you can kill it using sudo bin/storm kill Topologyname or Navigating to storm UI with the URL http://localhost:8772. Kill the tolology. Port number of UI is defined in storm.yaml

12. Run the kafka producer from the eclipse with arguments as localhost:9092 localhost:2181 . I have already mentioned that parameter in com.hortonworks.tutorials.tutorial1.TruckEventsProducer.

13. See if spouts and bolts are generating data and inserting data in HBASE and HDFS and HIVE.

===================== HBASE and HIVE tables for this project ==================================

1. HBASE

hbase(main):001:0> create 'truck_events', 'events'  
hbase(main):002:0> create 'driver_dangerous_events', 'count'  
hbase(main):003:0> list  
hbase(main):004:0>

2. Hive. We should use default database as we mentioned it in truckevent_topology.properties file.

$hive 
$ show databases
$ use default
$ show tables

.Create below 2 tables.

create table truck_events_text_partition  
(driverId string,  
 truckId string,  
 eventTime timestamp,  
 eventType string,  
 longitude double,  
 latitude double)  
partitioned by (date string)  
ROW FORMAT DELIMITED  
FIELDS TERMINATED BY ',';

create table truck_events_text_partition_orc
(driverId string,
truckId string,
eventTime timestamp,
eventType string,
longitude double,
latitude double)
partitioned by (date string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
stored as orc tblproperties ("orc.compress"="NONE");

===================== Data Verifcation in Hbase after running Topologies

1. HBASE
hbase(main):001:0> list
hbase(main):002:0> count ‘truck_events’

2. Hive
select count(*) from truck_events.

3. HDFS.
See if folders are created as mentioned in topology.properties file.














