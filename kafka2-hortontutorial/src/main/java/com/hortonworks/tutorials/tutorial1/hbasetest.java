package com.hortonworks.tutorials.tutorial1;



import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;

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

public class hbasetest {

	// schema variables
	private static byte [] tableName = toBytes("Book");
	private static byte [] infoFamily = toBytes("info");
	private static byte [] titleColumn = toBytes("title");
	private static byte [] descriptionColumn = toBytes("description");
	
	private static byte [] authorFamily = toBytes("author");
	private static byte [] firstNameColumn = toBytes("first");
	private static byte [] lastNameColumn = toBytes("last");
	
	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);

		/////////////////////////////////////
		// 1. Create table 'Book' with two families 'info' and 'author'
		HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
		tableDescriptor.addFamily(new HColumnDescriptor(infoFamily));
		tableDescriptor.addFamily(new HColumnDescriptor(authorFamily));
		admin.createTable(tableDescriptor);
		System.out.println("------------------------------");
		System.out.println("1: Created table ["+Bytes.toString(tableName) +"] with 2 families: "+ 
				Bytes.toString(infoFamily) + " and " + Bytes.toString(authorFamily));
		
		HTable table = new HTable(conf, tableName);
		
		/////////////////////////////////////
		// 2. Add data
		System.out.println("------------------------------");
		System.out.println("2: Saving rows: ");
		saveRow(table, "1", "Faster than the speed love", "Long book about love.", "Brian", "Dog");
		saveRow(table, "2", "Long day", "Story about Monday.", "Emily", "Blue");
		saveRow(table, "3", "Flying Car", "Novel about airplanes.", "Phil", "High");
		
		/////////////////////////////////////		
		// 3. Retrieve and print to the screen an entire record with id '1'
		Get get = new Get(toBytes("1"));
		Result result = table.get(get);
		System.out.println("------------------------------");
		System.out.println("3: " + resultToString(result));
		
		/////////////////////////////////////		
		// 4. Only retrieve title and description for record with id '2' and print to the screen
		get = new Get(toBytes("2"));
		get.addColumn(infoFamily, titleColumn);
		get.addColumn(infoFamily, descriptionColumn);
		result = table.get(get);
		System.out.println("------------------------------");
		System.out.println("4: " + resultToString(result));

		/////////////////////////////////////		
		// 5. Change the last name of an author for the record with title 'Long Day' to 'Happy'. 
		//    Display the record on the screen to verify the change.
		Put put = new Put(toBytes("2"));
		put.add(authorFamily, lastNameColumn, toBytes("Happy"));
		table.put(put);
		
		get = new Get(toBytes("2"));
		get.addColumn(authorFamily, lastNameColumn);
		result = table.get(get);
		System.out.println("------------------------------");
		System.out.println("5: " + resultToString(result));
		
		/////////////////////////////////////		
		// remember to close the table to release all the resources
		table.close();
		
		System.out.println("------------------------------");
		/////////////////////////////////////		
		// 8. Drop the table 'Book'
		admin.disableTable(tableName);
		admin.deleteTable(tableName);
	}

	private static void saveRow(HTable table, String rowId, String title, String description, String first, String last) throws IOException {
		Put put = new Put(toBytes(rowId));
		put.add(infoFamily, titleColumn, toBytes(title));
		put.add(infoFamily, descriptionColumn, toBytes(description));
		put.add(authorFamily, firstNameColumn, toBytes(first));
		put.add(authorFamily, lastNameColumn, toBytes(last));
		table.put(put);
		
		System.out.println("Saved row with id [" + rowId + "]");
	}

	private static String resultToString(Result result) {
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append("Result with rowId [" + Bytes.toString(result.getRow()) + "]");
		
		if ( result.containsColumn(infoFamily, titleColumn)){
			strBuilder.append(", title=" + Bytes.toString(result.getValue(infoFamily, titleColumn)));
		}
		if ( result.containsColumn(infoFamily, descriptionColumn)){
			strBuilder.append(", description=" + Bytes.toString(result.getValue(infoFamily, descriptionColumn)));
		}
		if ( result.containsColumn(authorFamily, firstNameColumn)){
			strBuilder.append(", first name=" + Bytes.toString(result.getValue(authorFamily, firstNameColumn)));
		}
		if ( result.containsColumn(authorFamily, lastNameColumn)){
			strBuilder.append(", last name=" + Bytes.toString(result.getValue(authorFamily, lastNameColumn)));
		}
		
		return strBuilder.toString();
	}

}

