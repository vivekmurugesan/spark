package com.test.spark;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

import scala.Tuple2;

/**
 * 
 * @author vivek
 *
 */
public class PCAUtil {
	
	private String outputDir;
	private String userPrefFile;
	
	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Movie Lens Data PCA");
		
		JavaSparkContext sc = new JavaSparkContext(conf);

		PCAUtil util = new PCAUtil(args[0], args[1]);
		int pcaCountStart = Integer.parseInt(args[2]);
		int pcaCountEnd = Integer.parseInt(args[3]);
		
		System.out.println("Running for::" +" with pca count start:" + pcaCountStart + " end:" + pcaCountEnd);
		
		util.generatePrincipalComponentsForRange(sc, pcaCountStart, pcaCountEnd);
		
	}
	
	public PCAUtil(String outputDir, String userPrefFile) {
		this.outputDir = outputDir;
		this.userPrefFile = userPrefFile;
	}
	
	public void generatePrincipalComponentsForRange(JavaSparkContext sc, int pcaCountStart, int pcaCountEnd) {
		for(int i=pcaCountStart;i<=pcaCountEnd;i++)
			generateKPrincipalComponents(sc, i);
	}
	
	public void generateKPrincipalComponents(JavaSparkContext sc, int kComponents) {
		
		System.out.println("Started for pca count::" + kComponents);

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
		
		Map<Long, Integer> indexToUserIdMap = generateIndexToUserIdMap(vectorRdd);
		final Map<Integer, Long> userIdToIndexMap = new HashMap<>();
		indexToUserIdMap.keySet().forEach(x -> {
			userIdToIndexMap.put(indexToUserIdMap.get(x), x);
		});
		
		Map<Long, Integer> indexToUserIdMapBc = sc.broadcast(indexToUserIdMap).value();
		Map<Integer, Long> userIdToIndexMapBc = sc.broadcast(userIdToIndexMap).value();
		
		JavaPairRDD<Integer, IndexedRow> indexedRdd = vectorRdd.mapToPair(x -> {
			
			int userId = x._1;
			long index = userIdToIndexMapBc.get(userId);
			return new Tuple2<>(userId, new IndexedRow(index, x._2));
		});
		
		IndexedRowMatrix indexedMat = new IndexedRowMatrix(indexedRdd.values().rdd());
		
		RowMatrix rowMatrix = indexedMat.toRowMatrix();
		
		Matrix pcaMat = rowMatrix.computePrincipalComponents(kComponents);
		
		//RowMatrix projected = rowMatrix.multiply(pcaMat);
		
		IndexedRowMatrix projected = indexedMat.multiply(pcaMat);
		
		JavaPairRDD<Integer, Vector> pcaRdd = projected.rows().toJavaRDD().mapToPair(x -> {
			long userIdIndex = x.index();
			int userId = indexToUserIdMapBc.get(userIdIndex);
			Vector components = x.vector();
			return new Tuple2<>(userId, components);
		});
		
		pcaRdd.map(x -> {
			StringBuilder sb = new StringBuilder();
			int userId = x._1;
			sb.append(userId);
			Vector pca = x._2;
			for(int i=0;i<pca.size();i++)
				sb.append(",").append(pca.apply(i));
			return sb.toString();
		}).saveAsTextFile(outputDir+"/PCA-"+kComponents);
		

	}
	
	public Map<Long, Integer> generateIndexToUserIdMap(JavaPairRDD<Integer, Vector> vectorRdd){
		
		List<Integer> userIds = vectorRdd.keys().collect();
		Map<Long, Integer> indexToUserIdMap = new HashMap<>();
		
		long index=0;
		for(Integer userId : userIds ) {
			indexToUserIdMap.put(index, userId);
			index++;
		}
		
		return indexToUserIdMap;
	}

}
