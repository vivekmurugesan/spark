package com.test.spark;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.Tuple3;

/**
 * 
 * @author vivek
 *
 */
public class MovieLensDataProc implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private List<String> allGenreList;
	
	private String outputDir;
	private String ratingFilePath;
	private String moviesFilePath;
	

	public static void main(String[] args) throws FileNotFoundException {

		SparkConf conf = new SparkConf().setAppName("Movie Lens Data Processing");
		
		JavaSparkContext sc = new JavaSparkContext(conf);

		MovieLensDataProc proc = new MovieLensDataProc(args[0], args[1], args[2]);
		proc.processMovieLensData(sc);
		
	}
	
	public MovieLensDataProc(String outputDir, String ratingFile, String moviesFile) {
		this.outputDir = outputDir;
		this.ratingFilePath = ratingFile;
		this.moviesFilePath = moviesFile;
		this.allGenreList = new ArrayList<>();
	}
	
	public void processMovieLensData(JavaSparkContext sc) throws FileNotFoundException {
		// Loading Movies data --> movie_id::title::tag1|tag2...
		JavaRDD<String> moviesFile = sc.textFile(moviesFilePath).cache();
		JavaPairRDD<Integer, Movie> moviesRdd = 
				moviesFile.mapToPair(x -> {
					String[] tokens = x.split("::");
					int movieId = Integer.parseInt(tokens[0]);
					Movie movie = new Movie(movieId, tokens[1]);
					String[] genres = tokens[2].split("\\|");
					List<String> genreList = new ArrayList<>();
					for(String t : genres)
						genreList.add(t);
					movie.setGenres(genreList);

					return new Tuple2<>(movieId, movie);
				}).cache();

		// Loading ratings data --> UserID::MovieID::Rating::Timestamp
		JavaRDD<String> ratingsFile = sc.textFile(ratingFilePath).cache();
		// <MovieId, <UserId, Rating>>
		JavaPairRDD<Integer, Tuple2<Integer, Double>> ratingsRdd = 
				ratingsFile.mapToPair(x -> {
					String[] tokens = x.split("::");
					int userId = Integer.parseInt(tokens[0]);
					int movieId = Integer.parseInt(tokens[1]);
					double rating = Double.parseDouble(tokens[2]);
					
					return new Tuple2<>(movieId, new Tuple2<>(userId, rating));
				}).cache();
		
		// Genre Summary on movies..
		Map<String, Long> genreWiseMovieCount = 
		moviesRdd.flatMapToPair(x -> {
			
			Movie m = x._2;
			List<Tuple2<Integer, String>> tuples = new ArrayList<>();
			for(String g : m.getGenres()) {
				tuples.add(new Tuple2<>(x._1, g));
			}
			return tuples.iterator();
		}).map(x -> x._2).countByValue();
		List<String> toPrint = new ArrayList<>();
		genreWiseMovieCount.entrySet().forEach(x -> toPrint.add(x.getKey()+","+ x.getValue()));
		
		sc.parallelize(toPrint).saveAsTextFile(outputDir +"/GenreMovieCount");
		
		// Top 20 Popular movies..
		JavaPairRDD<Integer, Integer> moviesRatingCount = 
				ratingsRdd.mapToPair(x -> new Tuple2<>(x._1,1)).reduceByKey((x,y) -> x+y);
		joinWithMoviesAndPrint(sc, moviesRatingCount, moviesRdd, "RatingCount");
		
		
		moviesRatingCount.saveAsTextFile(outputDir +"/RatingCount");
		
		JavaPairRDD<Integer, Double> ratingSumRdd = ratingsRdd.mapToPair(x -> new
				Tuple2<>(x._1, x._2._2)).foldByKey(0.0, (x,y) -> x+y );
		joinWithMoviesAndPrintWithCount(sc, 
				ratingSumRdd.join(moviesRatingCount), moviesRdd, "CumulativeRating");

		JavaPairRDD<Integer, Tuple2<Double, Integer>> ratingAvgRdd =
				ratingSumRdd.join(moviesRatingCount).mapToPair(x -> new Tuple2<>(x._1,
						new Tuple2<>(x._2._1/(1.0 *x._2._2),x._2._2)));
		
		ratingAvgRdd.map(x -> x._1 + "," + x._2._2+"," +x._2._1).saveAsTextFile(outputDir +"/RatingCountAvg");
		
		joinWithMoviesAndPrintWithCount(sc, ratingAvgRdd, moviesRdd, "AvgRating");

		joinWithMoviesAndPrintWithCount(sc, ratingAvgRdd.filter(x -> x._2._2 >= 10000), moviesRdd, "AvgRating_AtLeast10K");
		
		// Populating the all possible genre list from the data..
		this.createAllGenreList(sc, moviesRdd);
		
		joinWithMoviesForGenreWiseStat(sc, moviesRatingCount, moviesRdd, "RatingCount", this.allGenreList);
		
		joinWithMoviesForGenreWiseStatWithCount(sc, 
				ratingSumRdd.join(moviesRatingCount), moviesRdd, 
				"CumulativeRating", this.allGenreList);
		
		joinWithMoviesForGenreWiseStatWithCount(sc, ratingAvgRdd, moviesRdd, 
				"AvgRating", this.allGenreList);
		
		joinWithMoviesForGenreWiseStatWithCount(sc, ratingAvgRdd.filter(x -> x._2._2 >= 10000), 
				moviesRdd, "AvgRating_AtLeast10K", this.allGenreList);
		
		generateUserGenrePreferences(sc, ratingsRdd, moviesRdd);

		//generateUserGenrePreferences1(sc, ratingsRdd, moviesRdd);

	}
	
	
	
	private void generateUserGenrePreferences1(JavaSparkContext sc, JavaPairRDD<Integer, Tuple2<Integer, Double>> ratingsRdd,
			JavaPairRDD<Integer, Movie> moviesRdd) {
		// <UserId, MovieId>
				JavaPairRDD<Integer, Integer> userToMovieId = ratingsRdd.mapToPair(x -> new Tuple2<>(x._2._1, x._1));
				JavaPairRDD<Integer, Integer> userRatingCount = userToMovieId.mapToPair(x -> new Tuple2<>(x._1, 1)).reduceByKey((x,y) -> x+y);
				
				final List<String> genreListCopy = sc.broadcast(this.allGenreList).getValue();
				
				
				JavaPairRDD<Integer, Map<String, Integer>> userToGenreFreq = 
						userToMovieId.mapToPair(x -> new Tuple2<>(x._2, x._1)).join(moviesRdd).mapToPair(x -> x._2).groupByKey().mapToPair(x -> {
					int userId =x._1;
					Map<String, Integer> genreFreqMap = new HashMap<>();
					for(String g : genreListCopy)
						genreFreqMap.put(g, 0);
					
					System.out.println("For user:" + userId);
					for(Movie m : x._2) {
						if(userId % 100 == 0) 
							System.out.println(m);
						for(String g : m.getGenres()) {
							if(g.contains(Movie.EXCLUDE))
								break;
							String gL = g.trim().toLowerCase();
							genreFreqMap.put(gL, genreFreqMap.get(gL)+1);
						}
					}
					
					if(userId % 100 == 0) 
						System.out.println("Genre FreqMap for:" + userId + "::" + genreFreqMap);
					
					return new Tuple2<>(userId, genreFreqMap);
				});
				
				userToGenreFreq.saveAsTextFile(outputDir +"/FreqMap");
	}
	
	private void generateUserGenrePreferences(JavaSparkContext sc, JavaPairRDD<Integer, Tuple2<Integer, Double>> ratingsRdd,
			JavaPairRDD<Integer, Movie> moviesRdd) {


		// <UserId, MovieId>
		JavaPairRDD<Integer, Integer> userToMovieId = ratingsRdd.mapToPair(x -> new Tuple2<>(x._2._1, x._1));
		JavaPairRDD<Integer, Integer> userRatingCount = userToMovieId.mapToPair(x -> new Tuple2<>(x._1, 1)).reduceByKey((x,y) -> x+y);
		
		userRatingCount.map(x -> x._1+","+x._2).saveAsTextFile(outputDir +"/user-rating-counts");

		JavaPairRDD<Integer, Movie> userToMovie = userToMovieId.mapToPair(x -> new Tuple2<>(x._2,x._1)).join(moviesRdd)
				.mapToPair(x -> new Tuple2<>(x._2._1, x._2._2));
		JavaPairRDD<Integer, String> userToGenreFlatList = 
				userToMovie.flatMapToPair(x -> {
					int userId = x._1;
					List<String> genres = x._2.getGenres();
					List<Tuple2<Integer, String>> tuples = new ArrayList<>();
					for(String t : genres) {
						tuples.add(new Tuple2<>(userId, t));
					}
					return tuples.iterator();
				}).filter(x -> !x._2.contains(Movie.EXCLUDE));
		
		System.out.println("... User to Genre list count..::" + userToGenreFlatList.count());
		
		JavaPairRDD<Integer, Tuple2<String, Integer>> userToGenreFreq = 
				userToGenreFlatList.mapToPair(x -> new Tuple2<>(x._1+"_"+x._2,1)).reduceByKey((x,y) -> x+y).mapToPair(x -> {
					String[] tokens = x._1.split("_");
					int userId = Integer.parseInt(tokens[0]);
					String genre = tokens[1];
					return new Tuple2<>(userId, new Tuple2<>(genre, x._2));
				});
		
		final List<String> genreListCopy = sc.broadcast(this.allGenreList).getValue();
		
		JavaRDD<String> toPrintFreq = userToGenreFreq.groupByKey().mapToPair(x -> {
			int userId =x._1;
			Map<String, Integer> genreFreqMap = new HashMap<>();
			for(String g : genreListCopy)
				genreFreqMap.put(g, 0);
			
			for(Tuple2<String, Integer> y : x._2) {
				genreFreqMap.put(y._1, y._2);
			}
			
			if(userId % 100 == 0)
				System.out.println("Genre FreqMap for:" + userId + "::" + genreFreqMap);
			
			return new Tuple2<>(userId, genreFreqMap);
		}).join(userRatingCount).map(x -> {
			int userId = x._1;
			StringBuilder buf = new StringBuilder();
			buf.append(userId);
			for(String g : x._2._1.keySet())
				buf.append(",").append(x._2._1.get(g));
			buf.append(","+x._2._2);
			return buf.toString();
		});
		
		toPrintFreq.saveAsTextFile(outputDir +"/user-genre-pref-freqs");
		
		
		JavaPairRDD<Integer, Tuple2<String, Double>> userToGenreScore =
		userToGenreFreq.join(userRatingCount).mapToPair(x -> new Tuple2<>(x._1, new Tuple2<>(x._2._1._1, (x._2._1._2*1.0)/x._2._2)));
		
		
		
		JavaRDD<String> toPrint = userToGenreScore.groupByKey().mapToPair(x -> {
			int userId =x._1;
			Map<String, Double> genreScoreMap = new HashMap<>();
			for(String g : genreListCopy)
				genreScoreMap.put(g, 0.0);
			
			for(Tuple2<String, Double> y : x._2) {
				genreScoreMap.put(y._1, y._2);
			}
			
			return new Tuple2<>(userId, genreScoreMap);
		}).join(userRatingCount).map(x -> {
			int userId = x._1;
			StringBuilder buf = new StringBuilder();
			buf.append(userId);
			for(String g : x._2._1.keySet())
				buf.append(",").append(x._2._1.get(g));
			buf.append(","+x._2._2);
			return buf.toString();
		});
		
		toPrint.saveAsTextFile(outputDir +"/user-genre-pref-scores");

		StringBuilder headerBuf = new StringBuilder();
		headerBuf.append("UserId");
		for(String g : genreListCopy)
			headerBuf.append(",").append(g);
		PrintStream ps;
		try {
			ps = new PrintStream(new FileOutputStream(outputDir + "/header.csv"));
			ps.print(headerBuf.toString());
			ps.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	private void createAllGenreList(JavaSparkContext sc, JavaPairRDD<Integer, Movie> moviesRdd) {
		this.allGenreList.addAll(moviesRdd.flatMapToPair(x -> {
			int movieId = x._1;
			List<String> genres = x._2.getGenres();
			List<Tuple2<Integer, String>> tuples = new ArrayList<>();
			for(String g : genres) {
				tuples.add(new Tuple2<>(movieId, g.trim()));
			}
			
			return tuples.iterator();
		}).map(x -> x._2).filter(x -> !x.contains(Movie.EXCLUDE)).distinct().collect());
	}
	
	private void joinWithMoviesAndPrint(JavaSparkContext sc, JavaPairRDD<Integer, Integer> moviesStatRdd, JavaPairRDD<Integer, Movie> moviesRdd,
			String type) {
		List<Tuple2<Integer, Tuple2<Integer, Movie>>>  top20Movies = 
				moviesStatRdd.join(moviesRdd).takeOrdered(20, new SerializableComparator());
		JavaRDD<Tuple2<Integer, Tuple2<Integer, Movie>>> top20MoviesRdd = 
				sc.parallelize(top20Movies);
		
		top20MoviesRdd.map(x -> x._1 +"," + x._2._1 + "," + x._2._2).saveAsTextFile(outputDir+"/Top20-"+type);;
		
	}
	
	private void joinWithMoviesForGenreWiseStat(JavaSparkContext sc, JavaPairRDD<Integer, Integer> moviesStatRdd, 
			JavaPairRDD<Integer, Movie> moviesRdd,
			String type, List<String> genreList) {
		
		JavaPairRDD<Integer, Tuple2<String, Integer>> movieStatGenreRdd = 
		moviesStatRdd.join(moviesRdd).flatMapToPair(x -> {
			List<String> genres = x._2._2.getGenres();
			List<Tuple2<Integer,Tuple2<String, Integer>>> tuples = new ArrayList<>();
			
			for(String g : genres) {
				tuples.add(new Tuple2<>(x._1, new Tuple2<>(g, x._2._1)));
			}
			
			return tuples.iterator();
		});
		
		for(String genre : genreList) {
			JavaPairRDD<Integer, Integer> generWiseMovieStat =
				movieStatGenreRdd.filter(x -> genre.equalsIgnoreCase(x._2._1))
				.mapToPair(x -> new Tuple2<>(x._1, x._2._2));
			
			List<Tuple2<Integer, Tuple2<Integer, Movie>>>  top20Movies = 
					generWiseMovieStat.join(moviesRdd).takeOrdered(20, new SerializableComparator());

			JavaRDD<Tuple2<Integer, Tuple2<Integer, Movie>>> top20MoviesRdd = 
					sc.parallelize(top20Movies);
			
			top20MoviesRdd.map(x -> x._1 +"," + x._2._1 + "," + x._2._2).saveAsTextFile(outputDir+
					"/GenreWise/" + genre+"/Top20-"+type);;
		}
		

	}
	
	private void joinWithMoviesAndPrintWithCount(JavaSparkContext sc, JavaPairRDD<Integer, Tuple2<Double, Integer>> moviesStatRdd, 
			JavaPairRDD<Integer, Movie> moviesRdd, String type) {
		List<Tuple2<Integer, Tuple2<Tuple2<Double,Integer>, Movie>>>  top20Movies = 
				moviesStatRdd.join(moviesRdd).takeOrdered(20, new SerializableComparatorDouble());

		JavaRDD<Tuple2<Integer, Tuple2<Tuple2<Double,Integer>, Movie>>> top20MoviesRdd = 
				sc.parallelize(top20Movies);
		
		top20MoviesRdd.map(x -> x._1 +"," + x._2._1._1+"," + x._2._1._2 + "," + x._2._2).saveAsTextFile(outputDir+"/Top20-"+type);;

	}
	
	private void joinWithMoviesForGenreWiseStatWithCount(JavaSparkContext sc, JavaPairRDD<Integer, Tuple2<Double, Integer>> moviesStatRdd, 
			JavaPairRDD<Integer, Movie> moviesRdd, 
			String type, List<String> genreList) {
		
		JavaPairRDD<Integer, Tuple3<String, Double, Integer>> movieStatGenreRdd = 
		moviesStatRdd.join(moviesRdd).flatMapToPair(x -> {
			List<String> genres = x._2._2.getGenres();
			List<Tuple2<Integer,Tuple3<String, Double, Integer>>> tuples = new ArrayList<>();
			
			for(String g : genres) {
				tuples.add(new Tuple2<>(x._1, new Tuple3<>(g, x._2._1._1, x._2._1._2)));
			}
			
			return tuples.iterator();
		});
		
		for(String genre : genreList) {
			JavaPairRDD<Integer, Tuple2<Double, Integer>> generWiseMovieStat =
				movieStatGenreRdd.filter(x -> genre.equalsIgnoreCase(x._2._1()))
				.mapToPair(x -> new Tuple2<>(x._1, new Tuple2<>(x._2._2(), x._2._3())));
			
			List<Tuple2<Integer, Tuple2<Tuple2<Double,Integer>, Movie>>>  top20Movies = 
					generWiseMovieStat.join(moviesRdd).takeOrdered(20, new SerializableComparatorDouble());

			JavaRDD<Tuple2<Integer, Tuple2<Tuple2<Double,Integer>, Movie>>> top20MoviesRdd = 
					sc.parallelize(top20Movies);
			
			top20MoviesRdd.map(x -> x._1 +"," + x._2._1._1+"," + x._2._1._2 + "," + x._2._2).saveAsTextFile(outputDir+
					"/GenreWise/" + genre+"/Top20-"+type);;
		}
		

	}
	
	static class SerializableComparator implements Serializable, Comparator<Tuple2<Integer, Tuple2<Integer, Movie>>> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public int compare(Tuple2<Integer, Tuple2<Integer, Movie>> o1, Tuple2<Integer, Tuple2<Integer, Movie>> o2) {
			return o2._2._1.compareTo(o1._2._1);
		}
		
	}

	static class SerializableComparatorDouble implements Serializable, Comparator<Tuple2<Integer, 
		Tuple2<Tuple2<Double, Integer>, Movie>>> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public int compare(Tuple2<Integer, Tuple2<Tuple2<Double, Integer>, Movie>> o1,
				Tuple2<Integer, Tuple2<Tuple2<Double, Integer>, Movie>> o2) {
			// TODO Auto-generated method stub
			return o2._2._1._1.compareTo(o1._2._1._1);
		}
		
	}

}
