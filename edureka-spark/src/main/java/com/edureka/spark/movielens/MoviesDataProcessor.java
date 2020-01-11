package com.edureka.spark.movielens;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.edureka.spark.data.model.movielens.MovieDetails;

import scala.Tuple2;

/**
 * 
 * @author vivek
 *
 */
public class MoviesDataProcessor {

	
	private String inputPath = "/mnt/bigdatapgp/edureka_549997/datasets/movie_dataset";
	private String outputPath = "output";
	
	private String ratingsFileName = "ratings.csv";
	private String movieDetailsFileName = "movies_metadata.csv";
	
	private String delim = ",";
	
	private int topN = 10;
		
	
	public static void main(String[] args) {
	
		SparkConf conf = new SparkConf().setAppName("Data Processing");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		MoviesDataProcessor processor = new MoviesDataProcessor(args[0], args[1]);
		
		processor.processRatings(sc);
	}
	
	public MoviesDataProcessor(String inputPath, String outputPath) {
		this.inputPath = inputPath;
		this.outputPath = outputPath;
	}
	
	public void processRatings(JavaSparkContext sc) {
		
		JavaRDD<String> ratingsStringRdd = sc.textFile(inputPath + ratingsFileName);
		
		ratingsStringRdd = ratingsStringRdd.filter(x -> !x.startsWith("userId"));
		
		JavaPairRDD<Integer, Double> ratingsRdd = ratingsStringRdd.mapToPair(x ->{
			String[] tokens = x.split(delim);
			int movieId = Integer.parseInt(tokens[1]);
			double rating = Double.parseDouble(tokens[2]);
			
			return new Tuple2<>(movieId, rating);
		});
		
		JavaPairRDD<Integer, Integer> ratingCountRdd = 
				ratingsRdd.mapToPair(x -> new Tuple2<>(x._1, 1))
				.reduceByKey((x,y) -> x+y);
		
		JavaPairRDD<Integer, Integer> ratingCountReversed = ratingCountRdd.mapToPair(x -> 
				new Tuple2<>(x._2, x._1));
		
		List<Tuple2<Integer, Integer>> top10Movies = 
				ratingCountReversed.sortByKey(new SerializableComparator()).top(topN);
		
		top10Movies.forEach(System.out::println);
		
		List<Tuple2<Integer, Integer>> top10Movies2 =
				ratingCountRdd.top(topN, new SerializableTupleComparator());
		
		top10Movies2.forEach(System.out::println);
		
		JavaPairRDD<Integer, MovieDetails> movieDetailsRdd = processMovieDetails(sc);
		
		JavaPairRDD<Integer, Tuple2<Integer, MovieDetails>> joined = ratingCountRdd.join(movieDetailsRdd);
		
		List<Tuple2<Integer, Tuple2<Integer, MovieDetails>>> ratingCountTopNList =
			joined.top(topN, new SerializableTupleComparator2());
		
		List<String> toPrint = new ArrayList<>();
		
		ratingCountTopNList.forEach(x ->
				{
					int movieId = x._1;
					int ratingCount = x._2._1;
					MovieDetails movie = x._2._2;
					toPrint.add(movieId+","+
							ratingCount+","+
							movie.getTitle()+","+
							movie.getBudget()+","+
							movie.getRevenue()+","+
							movie.getVoteCount()+","+
							movie.getVoteAverage()+",");
				});
		
		JavaRDD<String> toPrintRdd = sc.parallelize(toPrint);
		
		toPrintRdd.saveAsTextFile(outputPath+"/RatingCount");
		
	}
	
	public JavaPairRDD<Integer, MovieDetails> processMovieDetails(JavaSparkContext sc){
		
		JavaRDD<String> movieDetailsStr = sc.textFile(inputPath + movieDetailsFileName);
		
		movieDetailsStr = movieDetailsStr.filter(x -> !x.startsWith("adult,"));
		
		JavaPairRDD<Integer, MovieDetails> movieDetailsRdd = movieDetailsStr.mapToPair(x -> {
			MovieDetails movieObj = MovieDetailsParser.parse(x);
			return new Tuple2<>(movieObj.getId(), movieObj);
		});
		
		return movieDetailsRdd;
	}
	
	
	
	static class SerializableTupleComparator implements Comparator<Tuple2<Integer, Integer>>{

		@Override
		public int compare(Tuple2<Integer, Integer> o1, Tuple2<Integer, Integer> o2) {
			return o2._2.compareTo(o1._2);
		}
		
	}
	
	static class SerializableTupleComparator2 implements Comparator<Tuple2<Integer, Tuple2<Integer, MovieDetails>>>{

		@Override
		public int compare(Tuple2<Integer, Tuple2<Integer, MovieDetails>> o1,
				Tuple2<Integer, Tuple2<Integer, MovieDetails>> o2) {
			return o2._2._1.compareTo(o1._2._1);
		}
		
	}
	
	
	static class SerializableComparator implements Comparator<Integer>{

		@Override
		public int compare(Integer o1, Integer o2) {
			return o2.compareTo(o1);
		}
		
	}

}
