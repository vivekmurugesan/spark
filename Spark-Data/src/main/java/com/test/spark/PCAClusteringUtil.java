package com.test.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;

import scala.Tuple2;

/**
 * 
 * @author vivek
 *
 */
public class PCAClusteringUtil {

	private String pcaFile;
	private String outputDir;
	private String userPrefFile;

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Movie Lens PCA Data Clustering");
		
		JavaSparkContext sc = new JavaSparkContext(conf);

		PCAClusteringUtil util = new PCAClusteringUtil(args[0], args[1], args[2]);
		int numIterations = Integer.parseInt(args[3]);
		int clusterCountStart = Integer.parseInt(args[4]);
		int clusterCountEnd = Integer.parseInt(args[5]);

		System.out.println("Running for::" + numIterations+ " iterations with start:" + clusterCountStart + " end:" + clusterCountEnd);

		util.applyClusteringOnUserPrefScores(sc, numIterations, clusterCountStart, clusterCountEnd);
	}

	public PCAClusteringUtil(String outputDir, String userPrefFile, String pcaFile) {
		this.pcaFile = pcaFile;
		this.outputDir = outputDir;
		this.userPrefFile = userPrefFile;
	}

	public double applyClusteringOnUserPrefScores(JavaSparkContext sc, int numClusters, int numIterations) {

		System.out.println("Started for cluster count::" + numClusters);

		JavaRDD<String> text = sc.textFile(this.userPrefFile).cache();

		JavaPairRDD<Integer, Vector> userPrfVectorRdd = text.mapToPair(x -> {
			String[] tokens = x.split(",");
			int userId = Integer.parseInt(tokens[0]);
			double[] arr = new double[tokens.length-2];
			// Skipping the last param which is the total rating count
			for(int i=1;i<tokens.length-1;i++) {
				arr[i-1] = Double.parseDouble(tokens[i]);
			}
			return new Tuple2<>(userId, new DenseVector(arr));
		});
		
		JavaRDD<String> text1 = sc.textFile(this.pcaFile).cache();

		JavaPairRDD<Integer, Vector> pcaVectorRdd = text1.mapToPair(x -> {
			String[] tokens = x.split(",");
			int userId = Integer.parseInt(tokens[0]);
			double[] arr = new double[tokens.length-2];
			// Skipping the last param which is the total rating count
			for(int i=1;i<tokens.length-1;i++) {
				arr[i-1] = Double.parseDouble(tokens[i]);
			}
			return new Tuple2<>(userId, new DenseVector(arr));
		});

		

		MultivariateStatisticalSummary summary = 
				Statistics.colStats(pcaVectorRdd.values().rdd());
		double[] means = summary.mean().toArray();
		double[] variances = summary.variance().toArray();
		double[] sd = new double[variances.length];
		for(int i=0;i<variances.length;i++)
			sd[i] = Math.sqrt(variances[i]);

		JavaPairRDD<Integer,Vector> scaledData = scaleData(pcaVectorRdd,
				means, sd);


		KMeansModel model = KMeans.train(scaledData.values().rdd(), numClusters, numIterations);

		System.out.println("Cluster centers:");
		for (Vector center: model.clusterCenters()) {
			System.out.println(" " + center);
		}
		double cost = model.computeCost(scaledData.values().rdd());
		System.out.println("Cost: " + cost);

		// Evaluate clustering by computing Within Set Sum of Squared Errors
		double WSSSE = model.computeCost(scaledData.values().rdd());
		System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

		System.out.println("Stat::"+numIterations+","+numClusters+","+cost+","+WSSSE);
		/*try {
			PrintStream ps = new PrintStream(new FileOutputStream(outputDir+ "/" + numClusters+"_clusters_stats.csv"));
			ps.println(numIterations+","+numClusters+","+cost+","+WSSSE);
			ps.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} */


		KMeansModel modelBroadcasted = sc.broadcast(model).getValue();

		JavaPairRDD<Integer, Integer> clusterRdd = scaledData.mapToPair(x -> {
			Integer id = x._1;
			int clusterId = modelBroadcasted.predict(x._2);
			return new Tuple2<>(id, clusterId);
		});
		
		JavaPairRDD<Integer, Tuple2<Integer, Vector>> clusterRddRawData = clusterRdd.join(userPrfVectorRdd);

		this.printSummary(clusterRdd, model, scaledData.values(), means, sd, numClusters, clusterRddRawData,
				sc);

		JavaPairRDD<Integer, Tuple2<Integer, Vector>> dataClusterIdRdd =  
				clusterRdd.join(pcaVectorRdd);

		JavaRDD<String> toPrint = dataClusterIdRdd.map(x ->{
			StringBuilder builder = new StringBuilder();
			builder.append(x._1).append(',').append(x._2._1.intValue());
			double[] data = x._2._2.toArray();
			for(double y : data){
				builder.append(',').append(y);
			}

			return builder.toString();
		});

		toPrint.saveAsTextFile(outputDir + "/" + numClusters + "_clusters/clustered_data");

		JavaRDD<String> toPrintClusterIds = dataClusterIdRdd.map(x -> {
			StringBuilder builder = new StringBuilder();
			builder.append(x._1).append(',').append(x._2._1.intValue());
			return builder.toString();
		});

		toPrintClusterIds.saveAsTextFile(outputDir + "/" + numClusters+ "_clusters/clusterIds");

		System.out.println("Completed for cluster count:" + numClusters);

		return cost;
	}

	public void applyClusteringOnUserPrefScores(JavaSparkContext sc,
			int numIterations, int clusterCountStart, int clusterCountEnd) {
		List<String> costSummary = new ArrayList<>();
		for(int i=clusterCountStart;i<=clusterCountEnd; i++) {
			StringBuilder bf = new StringBuilder();
			double cost = applyClusteringOnUserPrefScores(sc, i, numIterations);
			bf.append(numIterations + ",").append(i+",").append(cost);
			costSummary.add(bf.toString());
		}
		
		sc.parallelize(costSummary).saveAsTextFile(outputDir+"/Clustering-Cost-Summary");
	}

	private JavaPairRDD<Integer,Vector> scaleData(JavaPairRDD<Integer, Vector> dataVecRdd, double[] means,
			double[] sd){

		JavaPairRDD<Integer,Vector> scaledRdd = dataVecRdd.mapToPair(x -> {
			double[] data = x._2.toArray();
			double[] scaled = new double[data.length];
			for(int i=0;i<data.length;i++){
				scaled[i] = (data[i]-means[i])/sd[i];
			}

			Vector result = new DenseVector(scaled);
			return new Tuple2<>(x._1,result);
		});

		return scaledRdd;
	}

	private void printSummary(JavaPairRDD<Integer, Integer> clusterRdd, KMeansModel clusters, JavaRDD<Vector> data,
			double[] means, double[] sd, int numClusters,
			JavaPairRDD<Integer, Tuple2<Integer, Vector>> clusterRddRawData, JavaSparkContext sc){
		
		System.out.println("Cluster centers:");
		for (Vector center: clusters.clusterCenters()) {
			System.out.println(" " + center);
		}
		double cost = clusters.computeCost(data.rdd());
		System.out.println("Cost: " + cost);

		// Evaluate clustering by computing Within Set Sum of Squared Errors
		double WSSSE = clusters.computeCost(data.rdd());
		System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

		Map<Integer, Long> clusterCounts = 
				clusterRdd.mapToPair(x -> x.swap()).countByKey();

		// Denormalized/descaled cluster centers
		
		List<String> clusterCenterList = new ArrayList<>();
		
		JavaPairRDD<Integer, Vector> clusteredRawData = clusterRddRawData.mapToPair(x -> new Tuple2<>(x._2._1, x._2._2));
		
		Vector first = clusteredRawData.first()._2;
		int n = first.size();
		double[] zeroData = new double[n];
		Arrays.fill(zeroData, 0.0);
		Vector zeroVal = new DenseVector(zeroData);
		Map<Integer, Vector> clusterDataSum = clusteredRawData.foldByKey(zeroVal, (x,y) -> {
			double[] xData = x.toArray();
			double[] yData = y.toArray();
			double[] result = new double[xData.length];
			for(int i=0;i<xData.length;i++) {
				result[i] = xData[i] + yData[i];
			}
			
			return new DenseVector(result);
		}).collectAsMap();

		Map<Integer, Long> clusterItemCount = clusteredRawData.countByKey();
		
		Map<Integer, Vector> clusterMean = new HashMap<>();
		for(Integer clusterId : clusterDataSum.keySet()) {
			double[] sumData = clusterDataSum.get(clusterId).toArray();
			long count = clusterItemCount.get(clusterId);
			
			double[] meanData = new double[sumData.length];
			StringBuilder bf = new StringBuilder();
			bf.append(clusterId);
			
			for(int i=0;i<sumData.length;i++) {
				meanData[i] = sumData[i]/count;
				bf.append(","+meanData[i]);
			}
			bf.append(","+ count);
			
			clusterMean.put(clusterId, new DenseVector(meanData));
			
			System.out.println("\t member_count:" +
					 clusterCounts.get(clusterId)); 
			clusterCenterList.add(bf.toString());
		}
		
		
		/*
		 * for (Vector center: clusters.clusterCenters()) { StringBuilder bf = new
		 * StringBuilder(); bf.append(clusterId); double[] dataArr = center.toArray();
		 * System.out.printf("Cluster-center:::"); for(int i=0;i<dataArr.length;i++){
		 * double denorm = (dataArr[i]*sd[i]) + means[i]; System.out.printf("%f\t",
		 * denorm ); bf.append(","+denorm); } bf.append("," +
		 * clusterCounts.get(clusterId)); System.out.println("\t member_count:" +
		 * clusterCounts.get(clusterId)); clusterCenterList.add(bf.toString());
		 * clusterId++; }
		 */

		JavaRDD<String> toPrint = 
				sc.parallelize(clusterCenterList);
		toPrint.saveAsTextFile(this.outputDir + "/" + numClusters + "_clusters/cluster_centers");
		
	}

}
