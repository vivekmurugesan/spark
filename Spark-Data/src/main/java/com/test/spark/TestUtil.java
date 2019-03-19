package com.test.spark;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * 
 * @author vivek
 *
 */
public class TestUtil {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Test Util");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		loadTestData(sc);
	}
	
	private static void loadTestData(JavaSparkContext sc) {
		
		Random rand = new Random(123);
		List<Integer> list = new ArrayList<>();
		for(int i=0;i<1000*1000;i++)
			list.add(rand.nextInt(1000));
		
		JavaRDD<Integer> intRdd = sc.parallelize(list);
		System.out.println("Number of unique values: " +intRdd.distinct().count());
		intRdd.countByValue().forEach((x,y) -> {
			if(x % 100 == 0)
				System.out.printf("Number of Occurrences of:%d is:%d \n",x,y);
		});
		
		int u=50,sd=10;
		List<Double> listD = new ArrayList<>();
		for(int i=0;i<1000*1000;i++)
			listD.add(u+sd*rand.nextGaussian());
		
		JavaRDD<Double> rdd = sc.parallelize(listD);
		
		JavaDoubleRDD doubleRdd = rdd.mapToDouble(x -> x);
		System.out.printf("Stats of the rdd:: mean: %f and sd: %f \n ", doubleRdd.mean()
				, Math.sqrt(doubleRdd.variance()));
		
		Tuple2<double[], long[]> hist = doubleRdd.histogram(50);
		double[] buckets = hist._1;
		long[] freq = hist._2;
		System.out.printf("Buckets:%d\tFreqs:%d", buckets.length, freq.length);
		int draw = 0x2501;
		
		for(int i=1;i<buckets.length;i++) {
			System.out.printf("%s-%s\t",String.format("%.2f", buckets[i-1]),
					String.format("%.2f", buckets[i]));
			for(int j=1;j<freq[i-1];j++)
				if(j%500==0)
					System.out.printf("%c",(char)draw);
			System.out.println("|"+freq[i-1]);
		}
		
	}

}
