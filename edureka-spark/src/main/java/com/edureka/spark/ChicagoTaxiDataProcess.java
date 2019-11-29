package com.edureka.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.edureka.spark.data.model.ODPairSummary;
import com.edureka.spark.data.model.TripDetails;
import com.google.common.collect.Iterables;

import scala.Tuple2;
/**
 * 
 * @author vivek
 *
 */
public class ChicagoTaxiDataProcess {

	private String inputPath = "taxi_trip_community_od_input";
	private String outputPath = "output";
	
	private static final String NA = "\\N";
	
	private static final String delim = "\t";
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("Data Processing");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		ChicagoTaxiDataProcess processor = new ChicagoTaxiDataProcess(args[0], args[1]);
		
		JavaRDD<ODPairSummary> odSummaryRdd = processor.processData(sc);
		processor.saveResults(odSummaryRdd);
	}
	
	public ChicagoTaxiDataProcess(String inputPath, String outputPath) {
		this.inputPath = inputPath;
		this.outputPath = outputPath;
	}
	
	public void saveResults(JavaRDD<ODPairSummary> odSummaryRdd) {
		odSummaryRdd.map(x -> x.toString()).saveAsTextFile(this.outputPath);
	}
	
	public JavaRDD<ODPairSummary> processData(JavaSparkContext sc){
		JavaRDD<TripDetails> tripsRdd = readFile(sc, this.inputPath);
		
		JavaPairRDD<String, TripDetails> odPairTrips =  tripsRdd.mapToPair(x -> {
			return new Tuple2<>(x.getPikcupCommunity()+"__" + x.getDropoffCommunity(), x);
		});
		
		JavaPairRDD<String, Iterable<TripDetails>> odPairGrouped = 
				odPairTrips.groupByKey();
		
		JavaPairRDD<String, ODPairSummary> odPairSummaryRdd = odPairGrouped.mapToPair(x ->{
			
			Iterable<TripDetails> tripsDetailsList = x._2;
			int tripCount = Iterables.size(tripsDetailsList);
			
			double tripTotalAmt = 0.0;
			double tripTotalMiles = 0.0;
			int tripTotalMins = 0;
			
			for(TripDetails a : tripsDetailsList) {
				tripTotalAmt += a.getTripTotalAmt();
				tripTotalMiles += a.getTripMiles();
				tripTotalMins += Math.round(a.getTripSeconds()/60.0);
			}
			
			double avgAmt = tripTotalAmt/tripCount;
			double avgMiles = tripTotalMiles/tripCount;
			double avgMins = tripTotalMins * 1.0/tripCount;
			
			ODPairSummary result = new ODPairSummary();
			result.setTripCount(tripCount);
			result.setTotalAmount(tripTotalAmt);
			result.setTotalMiles(tripTotalMiles);
			result.setTotalMins(tripTotalMins);
			result.setAvgAmount(avgAmt);
			result.setAvgMiles(avgMiles);
			result.setAvgMins(avgMins);
			
			return new Tuple2<>(x._1, result);
		});
		
		return odPairSummaryRdd.values();
	}
	
	public JavaRDD<TripDetails> readFile(JavaSparkContext sc, String inputPath){
		
		JavaRDD<String> text = sc.textFile(inputPath);
		
		text = text.filter(x -> {
			String[] tokens = x.split(delim);
			if(tokens.length < 5)
				return false;
			
			if(tokens[0].contentEquals(NA) || tokens[1].contentEquals(NA)
					|| tokens[2].contentEquals(NA) || tokens[3].contentEquals(NA)
					|| tokens[4].contentEquals(NA))
				return false;
			else 
				return true;
		});
		
		JavaRDD<TripDetails> tripDetailsRdd = text.map(x -> {
			String[] tokens = x.split(delim);
			TripDetails tripDetails = new TripDetails();
			tripDetails.setPikcupCommunity(Integer.parseInt(tokens[0]));
			tripDetails.setDropoffCommunity(Integer.parseInt(tokens[1]));
			tripDetails.setTripTotalAmt(Double.parseDouble(tokens[2]));
			tripDetails.setTripMiles(Double.parseDouble(tokens[3]));
			tripDetails.setTripSeconds(Integer.parseInt(tokens[4]));
			return tripDetails;
		});
		
		return tripDetailsRdd;
	}

}
