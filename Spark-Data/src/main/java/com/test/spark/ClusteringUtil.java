package com.test.spark;

import java.util.ArrayList;
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
public class ClusteringUtil {
	
	protected String outputDir;
	protected String userPrefFile;
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Movie Lens Data Clustering");
		
		JavaSparkContext sc = new JavaSparkContext(conf);

		ClusteringUtil util = new ClusteringUtil(args[0], args[1]);
		int numIterations = Integer.parseInt(args[2]);
		int clusterCountStart = Integer.parseInt(args[3]);
		int clusterCountEnd = Integer.parseInt(args[4]);
		
		System.out.println("Running for::" + numIterations+ " iterations with start:" + clusterCountStart + " end:" + clusterCountEnd);
		
		util.applyClusteringOnUserPrefScores(sc, numIterations, clusterCountStart, clusterCountEnd);
		
	}

	public ClusteringUtil(String outputDir, String userPrefFile) {
		this.outputDir = outputDir;
		this.userPrefFile = userPrefFile;
	}
	
	public void applyClusteringOnUserPrefScores(JavaSparkContext sc, int numClusters, int numIterations) {
		
		System.out.println("Started for cluster count::" + numClusters);

		JavaRDD<String> text = sc.textFile(this.userPrefFile).cache();
		
		JavaPairRDD<Integer, Vector> vectorRdd = text.mapToPair(x -> {
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
                Statistics.colStats(vectorRdd.values().rdd());
		double[] means = summary.mean().toArray();
		double[] variances = summary.variance().toArray();
		double[] sd = new double[variances.length];
		for(int i=0;i<variances.length;i++)
		        sd[i] = Math.sqrt(variances[i]);
		
		JavaPairRDD<Integer,Vector> scaledData = scaleData(vectorRdd,
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
		
		
		KMeansModel modelBroadCasted = sc.broadcast(model).getValue();
        
        JavaPairRDD<Integer, Integer> clusterRdd = scaledData.mapToPair(x -> {
                Integer id = x._1;
                int clusterId = modelBroadCasted.predict(x._2);
                return new Tuple2<>(id, clusterId);
        });
        
        this.printSummary(clusterRdd, model, scaledData.values(), means, sd, numClusters,
                        sc);
        
        JavaPairRDD<Integer, Tuple2<Integer, Vector>> dataClusterIdRdd =  
                clusterRdd.join(vectorRdd);
        
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

	
	}
	
	public void applyClusteringOnUserPrefScores(JavaSparkContext sc,
			int numIterations, int clusterCountStart, int clusterCountEnd) {
		for(int i=clusterCountStart;i<=clusterCountEnd; i++) {
			applyClusteringOnUserPrefScores(sc, i, numIterations);
		}
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
            JavaSparkContext sc){
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
    int clusterId = 0;
    List<String> clusterCenterList = new ArrayList<>();
    for (Vector center: clusters.clusterCenters()) {
            StringBuilder bf = new StringBuilder();
            bf.append(clusterId);
            double[] dataArr = center.toArray();
            System.out.printf("Cluster-center:::");
            for(int i=0;i<dataArr.length;i++){
                    double denorm = (dataArr[i]*sd[i]) + means[i];
                    System.out.printf("%f\t", denorm );
                    bf.append(","+denorm);
            }
            bf.append("," + clusterCounts.get(clusterId));
            System.out.println("\t member_count:" + clusterCounts.get(clusterId));
            clusterCenterList.add(bf.toString());
            clusterId++;
    }
    
    JavaRDD<String> toPrint = 
                    sc.parallelize(clusterCenterList);
    toPrint.saveAsTextFile(this.outputDir + "/" + numClusters + "_clusters/cluster_centers");
}

}
