
Instructions:
Run the following commands in the by logging into the web console. 
These will ensure that the required files from the dataset are copied into hdfs.
				
Hdfs commands to copy the files into hdfs:

> cd /mnt/bigdatapgp/edureka_549997/datasets/movie_lens/ml-10M100K

> hdfs dfs -mkdir movie_lens
> hdfs dfs -put movies.dat movie_lens
> hdfs dfs -put ratings.dat movie_lens
				
Instructions:
Run,
>spark2-shell
from the command line by logging into the web console.
Run all the commands in the spark shell interactively to compute and view the required results.
Ensure that the files are copied onto hdfs by running the commands mentioned above before running these 
commands in spark shell.


Loading movies and ratings files to apply the required processin on them.

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

val moviesFilePath = "movie_lens/movies.dat"
val ratingsFilePath = "movie_lens/ratings.dat"
val delim = "::"
val genreDelim = "\\|"

val sqlContext = new org.apache.spark.sql.SQLContext(sc)

val moviesSchema = StructType(Array(
    StructField("movieId", StringType, true), 
    StructField("title", StringType, true), 
    StructField("genres", StringType, true)));
 
val moviesData = sc.textFile(moviesFilePath)


val moviesRowRdd = moviesData.map(line => Row.fromSeq(line.split(delim)))


val moviesDF = sqlContext.createDataFrame(moviesRowRdd, moviesSchema)

moviesDF.show


val ratingSchema = StructType(Array(
    StructField("userId", StringType, true), 
    StructField("movieId", StringType, true), 
    StructField("rating", DoubleType, true),
    StructField("timestamp", StringType, true)));
    
val ratingsData = sc.textFile(ratingsFilePath)

val ratingsRowRdd = ratingsData.map(line => line.split(delim)).map(tokens => Row(tokens(0), tokens(1), tokens(2).toDouble, tokens(3)))

val ratingsDF = sqlContext.createDataFrame(ratingsRowRdd, ratingSchema)

ratingsDF.show


1. Question 1: Genre wise count.
val movieGenresDF = moviesDF.select($"genres")
val movieGenresRdd = movieGenresDF.map(x => x.getString(0))
val movieGenresListRdd = movieGenresRdd.map(x => x.split(genreDelim))
val movieGenresFlattened = movieGenresListRdd.rdd.flatMap(x => x.toSeq)
val genresCount = movieGenresFlattened.map((_, 1L)).reduceByKey(_+_)

genresCount.toDF().select($"_1", $"_2").orderBy($"_2".desc).show


2. Question 2: Rating count for each movie.

ratingsDF.groupBy($"movieId").count.show

3. Question 3: Top 20 movies by rating count.

val movieRatingCountDF = ratingsDF.groupBy($"movieId").count

val joined = movieRatingCountDF.join(moviesDF, Seq("movieId", "movieId"), "inner")

joined.select($"movieId", $"count", $"title").orderBy($"count".desc).show(20)

4. Question 4: Top 20 movies based on average rating

val movieRatingMeanDF = ratingsDF.groupBy($"movieId").mean("rating")

val meanJoined = movieRatingMeanDF.join(moviesDF, Seq("movieId", "movieId"), "inner")

meanJoined.select($"movieId", $"avg(rating)", $"title").orderBy($"avg(rating)".desc).show(20)

5. Question 5: Top 20 movies based on average rating with at least 10K ratings

val movieRatingCountMeanRating = movieRatingMeanDF.join(movieRatingCountDF, Seq("movieId", "movieId"), "inner").join(moviesDF, Seq("movieId", "movieId"), "inner")

val movies10KRatings = movieRatingCountMeanRating.filter($"count" >= 10000)

movies10KRatings.select($"movieId", $"count", $"avg(rating)", $"title").orderBy($"avg(rating)".desc).show(20)

