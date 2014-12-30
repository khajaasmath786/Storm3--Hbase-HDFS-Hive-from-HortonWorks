package com.hortonworks.tutorials.tutorial1;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.*;
public class HbaseConfig {
    private static final Logger LOG = Logger.getLogger(HbaseConfig.class);
    public static org.apache.hadoop.conf.Configuration getHHConfig() {
        Configuration conf = HBaseConfiguration.create();
        InputStream confResourceAsInputStream = conf.getConfResourceAsInputStream("hbase-site.xml");
        int available = 0;
        try {
            available = confResourceAsInputStream.available();
        } catch (Exception e) {
            //for debug purpose
            LOG.debug("configuration files not found locally");
        } finally {
            IOUtils.closeQuietly(confResourceAsInputStream);
        }
        if (available == 0 ) {
            conf = new Configuration();
            //conf.addResource("core-site.xml");
            conf.addResource("hbase-site.xml");
           // conf.addResource("hdfs-site.xml");
        }
        return conf;
    }


public static void main(String[] args) throws IOException {
        Configuration config = HbaseConfig.getHHConfig();
        HTable table = new HTable(config, "s1");
        System.out.println(table.getTableName());
}}
