package com.test.graphx;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.lib.ConnectedComponents;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;
import scala.reflect.ClassTag;

public class ConnectionPairGraph {

	private String inputFile;
	private String outputDir;
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Graph Processing of passenger Pair data");
		
		JavaSparkContext sc = new JavaSparkContext(conf);

		ConnectionPairGraph graphUtil = new ConnectionPairGraph(args[0], args[1]);
		graphUtil.processData(sc);
	}

	public ConnectionPairGraph(String inputFile, String outputDir) {
		super();
		this.inputFile = inputFile;
		this.outputDir = outputDir;
	}
	
	public void processData(JavaSparkContext sc) {
		JavaRDD<String> text = sc.textFile(inputFile);
		JavaPairRDD<Integer, Integer> idPairRdd = text.mapToPair(x -> {
			String[] tokens = x.split(",");
			int id1 = Integer.parseInt(tokens[0]);
			int id2 = Integer.parseInt(tokens[1]);
			return new Tuple2<>(id1, id2);
		});
		
		JavaRDD<Integer> vertices = idPairRdd.keys().distinct().union(idPairRdd.values().distinct());
		
		/** Computing distinct pairs by counting the instances */
		JavaPairRDD<String, Integer> countRdd = 
				text.mapToPair(x -> new Tuple2<>(x,1)).reduceByKey((x,y) -> x+y);
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> weightedIdPair =
				countRdd.mapToPair(x -> {

					String[] tokens = x._1.split(",");
					int id1 = Integer.parseInt(tokens[0]);
					int id2 = Integer.parseInt(tokens[1]);
					return new Tuple2<>(new Tuple2<>(id1, id2), x._2);
				
				});
		
		JavaRDD<Edge<Integer>> edges = weightedIdPair.map(x -> {
			return new Edge<Integer>(x._1._1, x._1._1, x._2);
		});
		
		ClassTag<Integer> intTag = scala.reflect.ClassTag$.MODULE$.apply(Integer.class);

		
		Graph<Integer, Integer> idPairGraph = Graph.fromEdges( 
				edges.rdd(), 1, StorageLevel.MEMORY_AND_DISK(), 
				StorageLevel.MEMORY_AND_DISK(), intTag, intTag);
		
		System.out.println("Total number of vertices.." + idPairGraph.vertices().count());
		
		System.out.println("Total number of edges.." + idPairGraph.edges().count());
		
		
		Graph<Object, Integer> connectedComps = 
				ConnectedComponents.run(idPairGraph, 50, intTag, intTag);
		
		
		System.out.println("Total number of connected comps.." + connectedComps.vertices().count());
		
		
	}

}
