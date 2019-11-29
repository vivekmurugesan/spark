package com.edureka.hbase;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 * @author vivek
 *
 */
public class InsertData {
	
	// pickup_community, daily_trip_count, daily_total_amt, daily_total_miles, daily_total_mins, daily_avg_amt, daily_avg_miles, daily_avg_mins
	private static final String communityOrigSummaryFile = "taxi_trip_community_origin_summary_2/000000_0";
	
	// dropoff_community, daily_trip_count, daily_total_amt, daily_total_miles, daily_total_mins, daily_avg_amt, daily_avg_miles, daily_avg_mins
	private static final String communityDestSummaryFile = "taxi_trip_community_dropoff_summary_2/000000_0";
	
	// Company_Name, daily_trip_count, daily_total_amt, daily_total_miles, daily_total_mins, daily_avg_amt, daily_avg_miles, daily_avg_mins
	private static final String companySummaryFile = "taxi_trip_company_summary_2/000000_0";
	
	private static final String communityTableName = "CommunitySummary";
	private static final String[] communityColFamilies = {"OriginStats", "DestinationStats", "RevenueDetails"};
	
	private static final String companyTableName = "CompanySummary";
	private static final String[] companyColFamilies = {"TripCountStats", "RevenueDetails"};
	
	private static final String delim = "\t";
	
	public static void main(String[] args) {
		
		InsertData util = new InsertData();
		System.out.println(".. Insertion initiated..");
		try {
			util.insertData();
		} catch (IOException e) {
			System.err.println(".. Insertion failed.." + e);
			e.printStackTrace();
		}
		System.out.println(".. Insertion completed...");
	}
	
	public void insertData() throws IOException {
		
		insertCompanySummaryData();
		
		insertCommunityOrigSummaryData();
		insertCommunityOrigSummaryData();
				
	}
	
	public HTable createTableHandle(String tableName) throws IOException {
		Configuration conf = HBaseConfiguration.create();

		HTable hTable = new HTable(conf, tableName);
		
		return hTable;
	}
	
	public void insertCompanySummaryData() throws IOException {

		BufferedReader br = createReaderForFile(companySummaryFile);

		HTable hTable = createTableHandle(companyTableName);

		// Company_Name, daily_trip_count, daily_total_amt, daily_total_miles, daily_total_mins, daily_avg_amt, daily_avg_miles, daily_avg_mins
		String line = br.readLine();
		int index=1;

		System.out.println("Inserting company summary..");

		while(line != null && !line.isEmpty()) {
			System.out.println("inserting::" + index++);
			String[] tokens = line.split(delim);
			if(tokens.length < 8)
				continue;

			String companyName = (tokens[0] == null || tokens[0].isEmpty())?"Unknown":tokens[0];
			Put p = new Put(Bytes.toBytes(companyName)); // Company name as row id.

			// add values using add() method
			p.add(Bytes.toBytes(companyColFamilies[0]),
					Bytes.toBytes("daily_trip_count"),Bytes.toBytes(tokens[1]));

			p.add(Bytes.toBytes(companyColFamilies[1]),
					Bytes.toBytes("daily_total_amt"),Bytes.toBytes(tokens[2]));
			p.add(Bytes.toBytes(companyColFamilies[0]),
					Bytes.toBytes("daily_total_miles"),Bytes.toBytes(tokens[3]));
			p.add(Bytes.toBytes(companyColFamilies[0]),
					Bytes.toBytes("daily_total_mins"),Bytes.toBytes(tokens[4]));

			p.add(Bytes.toBytes(companyColFamilies[1]),
					Bytes.toBytes("daily_avg_amt"),Bytes.toBytes(tokens[5]));
			p.add(Bytes.toBytes(companyColFamilies[0]),
					Bytes.toBytes("daily_avg_miles"),Bytes.toBytes(tokens[6]));
			p.add(Bytes.toBytes(companyColFamilies[0]),
					Bytes.toBytes("daily_avg_mins"),Bytes.toBytes(tokens[7]));

			// save the put Instance to the HTable.
			hTable.put(p);
			line = br.readLine();
		}
		System.out.println("data inserted successfully for company..");

		// close HTable instance
		hTable.close();

	}
	
	public void insertCommunityOrigSummaryData() throws IOException {

		BufferedReader br = createReaderForFile(communityOrigSummaryFile);

		HTable hTable = createTableHandle(communityTableName);

		// pickup_community, daily_trip_count, daily_total_amt, daily_total_miles, daily_total_mins, daily_avg_amt, daily_avg_miles, daily_avg_mins
		String line = br.readLine();
		System.out.println("Inserting for community orig");
		int index = 1;

		while(line != null && !line.isEmpty()) {
			System.out.println("Inserting record::" + index++);
			String[] tokens = line.split(delim);
			if(tokens.length < 8){
				System.out.println("Ignoring.. line::" + line + " " + tokens.length);
				continue;
			}

			Put p = new Put(Bytes.toBytes(tokens[0])); // Community id as row id.

			// add values using add() method
			p.add(Bytes.toBytes(communityColFamilies[0]),
					Bytes.toBytes("daily_trip_count"),Bytes.toBytes(tokens[1]));

			p.add(Bytes.toBytes(communityColFamilies[2]),
					Bytes.toBytes("orig_daily_total_amt"),Bytes.toBytes(tokens[2]));
			p.add(Bytes.toBytes(communityColFamilies[0]),
					Bytes.toBytes("daily_total_miles"),Bytes.toBytes(tokens[3]));
			p.add(Bytes.toBytes(communityColFamilies[0]),
					Bytes.toBytes("daily_total_mins"),Bytes.toBytes(tokens[4]));

			p.add(Bytes.toBytes(communityColFamilies[2]),
					Bytes.toBytes("orig_daily_avg_amt"),Bytes.toBytes(tokens[5]));
			p.add(Bytes.toBytes(communityColFamilies[0]),
					Bytes.toBytes("daily_avg_miles"),Bytes.toBytes(tokens[6]));
			p.add(Bytes.toBytes(communityColFamilies[0]),
					Bytes.toBytes("daily_avg_mins"),Bytes.toBytes(tokens[7]));

			// save the put Instance to the HTable.
			hTable.put(p);
			line = br.readLine();

		}
		System.out.println("data inserted successfully for community orig");

		// close HTable instance
		hTable.close();

	}
	
	public void insertCommunityDestSummaryData() throws IOException {

		BufferedReader br = createReaderForFile(communityDestSummaryFile);

		HTable hTable = createTableHandle(communityTableName);

		// pickup_community, daily_trip_count, daily_total_amt, daily_total_miles, daily_total_mins, daily_avg_amt, daily_avg_miles, daily_avg_mins
		String line = br.readLine();

		while(line != null && !line.isEmpty()) {
			String[] tokens = line.split(delim);
			if(tokens.length < 8)
				continue;

			Put p = new Put(Bytes.toBytes(tokens[0])); // Community id as row id.

			// add values using add() method
			p.add(Bytes.toBytes(communityColFamilies[1]),
					Bytes.toBytes("daily_trip_count"),Bytes.toBytes(tokens[1]));

			p.add(Bytes.toBytes(communityColFamilies[2]),
					Bytes.toBytes("dest_daily_total_amt"),Bytes.toBytes(tokens[2]));
			p.add(Bytes.toBytes(communityColFamilies[1]),
					Bytes.toBytes("daily_total_miles"),Bytes.toBytes(tokens[3]));
			p.add(Bytes.toBytes(communityColFamilies[1]),
					Bytes.toBytes("daily_total_mins"),Bytes.toBytes(tokens[4]));

			p.add(Bytes.toBytes(communityColFamilies[2]),
					Bytes.toBytes("dest_daily_avg_amt"),Bytes.toBytes(tokens[5]));
			p.add(Bytes.toBytes(communityColFamilies[1]),
					Bytes.toBytes("daily_avg_miles"),Bytes.toBytes(tokens[6]));
			p.add(Bytes.toBytes(communityColFamilies[1]),
					Bytes.toBytes("daily_avg_mins"),Bytes.toBytes(tokens[7]));

			// save the put Instance to the HTable.
			hTable.put(p);
			line = br.readLine();
		}
		System.out.println("data inserted successfully");

		// close HTable instance
		hTable.close();

	}
	
	public BufferedReader createReaderForFile(String fileName) throws FileNotFoundException {
		
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
		
		return br;
	}
	
	

}
