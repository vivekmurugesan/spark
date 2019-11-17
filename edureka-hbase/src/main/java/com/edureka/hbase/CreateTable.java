package com.edureka.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class CreateTable {

	private Configuration config;
	
	public static void main(String[] args) {
	}
	
	public void init() {
		this.config = HBaseConfiguration.create();
	}
	
	public void createTable(HTableDescriptor tableDesc) {
		
	}

}
