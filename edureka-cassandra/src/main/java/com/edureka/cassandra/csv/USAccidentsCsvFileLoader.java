package com.edureka.cassandra.csv;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.edureka.cassandra.CassandraClient;

/**
 * 
 * @author vivek
 *
 */
public class USAccidentsCsvFileLoader {

	public static final String idCol = "ID";
	public static final String startTimeCol = "Start_Time";
	public static final String endTimeCol = "End_Time";
	
	public static final String startLatCol = "Start_Lat";
	public static final String startLngCol = "Start_Lng";
	public static final String endLatCol = "End_Lat"; 
	public static final String endLngCol = "End_Lng";
	
	// Address details
	public static final String numberCol = "Number";
	public static final String streetCol = "Street";
	public static final String sideCol = "Side";
	public static final String cityCol = "City";
	public static final String countyCol = "County";
	public static final String stateCol = "State";
	public static final String zipCol = "Zipcode";
	public static final String countryCol = "Country";
	
	// Weather details
	public static final String weatherTimestampCol = "Weather_Timestamp";
	public static final String temparatureCol= "Temperature(F)";
	public static final String windChillCol = "Wind_Chill(F)";
	public static final String humidityCol = "Humidity(%)";
	public static final String pressureCol = "Pressure(in)";
	public static final String visibilityCol = "Visibility(mi)";
	public static final String windDirectionCol = "Wind_Direction";
	public static final String windSpeedCol = "Wind_Speed(mph)";
	public static final String precipitationCol = "Precipitation(in)";
	public static final String weatherConditionCol = "Weather_Condition";
	
	private static final String inputFile = "/mnt/bigdatapgp/edureka_549997/datasets/US_accidents/US_Accidents_Dec19.csv";
	
	private CassandraClient client;
	
	private static final String nodeIp = "ip-20-0-31-210.ec2.internal";
	private static final Integer port = 9042;
	
	public static void main(String[] args) throws FileNotFoundException, IOException {
		USAccidentsCsvFileLoader loader = new USAccidentsCsvFileLoader();
		loader.parseAndProcessFile();
	}
	
	public USAccidentsCsvFileLoader() {
		this.client = new CassandraClient(nodeIp, port);
		this.client.connect();
		this.client.createSchema();
	}

	public void parseAndProcessFile() throws FileNotFoundException, IOException {
		File csvFile = new File(inputFile);
		CSVParser parser = 
				CSVFormat.DEFAULT.withFirstRecordAsHeader().withQuote(null).withDelimiter(',')
				.parse(new FileReader(csvFile));
		/*
		 * CSVParser parser = CSVParser.parse(csvFile, StandardCharsets.UTF_16BE,
		 * CSVFormat.POSTGRESQL_CSV);
		 */ 

		Map<String, Integer> headerMap = 
				parser.getHeaderMap();

		System.out.println("Headers..\n" + headerMap);

		List<CSVRecord> csvRecords = parser.getRecords();


		int i = 0;
		for(CSVRecord record : csvRecords) {
			System.out.printf(".. Processing Record....:%d",i);
			
			USAccidentDetails details = new USAccidentDetails();
			
			try {
				details.setId(record.get(headerMap.get(idCol)));

				details.setStartTime(record.get(headerMap.get(startTimeCol)));
				details.setEndTime(record.get(headerMap.get(endTimeCol)));

				details.setStartLat(record.get(headerMap.get(startLatCol)));
				details.setStartLng(record.get(headerMap.get(startLngCol)));
				details.setEndLat(record.get(headerMap.get(endLatCol)));
				details.setEndLng(record.get(headerMap.get(endLngCol)));

				details.setNumber(record.get(headerMap.get(numberCol)));
				details.setStreet(record.get(headerMap.get(streetCol)));
				details.setSide(record.get(headerMap.get(sideCol)));
				details.setCity(record.get(headerMap.get(cityCol)));
				details.setCounty(record.get(headerMap.get(countyCol)));
				details.setState(record.get(headerMap.get(stateCol)));
				details.setZip(record.get(headerMap.get(zipCol)));
				details.setCountry(record.get(headerMap.get(countryCol)));

				details.setWeatherTimestamp(record.get(headerMap.get(weatherTimestampCol)));
				details.setTemparature(record.get(headerMap.get(temparatureCol)));
				details.setWindChill(record.get(headerMap.get(windChillCol)));
				details.setHumidity(record.get(headerMap.get(humidityCol)));
				details.setPressure(record.get(headerMap.get(pressureCol)));
				details.setVisibility(record.get(headerMap.get(visibilityCol)));
				details.setWindDirection(record.get(headerMap.get(windDirectionCol)));
				details.setWindSpeed(record.get(headerMap.get(windSpeedCol)));
				details.setPrecipitation(record.get(headerMap.get(precipitationCol)));
				details.setWeatherCondition(record.get(headerMap.get(weatherConditionCol)));
				
				this.client.insertDataFrom(details);
				
			}catch(Exception e) {
				System.out.println(".. Exception in processing record:" + i);
				e.printStackTrace();
			}
			System.out.printf("........End of processing record : %d...........\n", i);
			i++;
		}
	}
}
