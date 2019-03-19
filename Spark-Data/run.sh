#!/bin/sh

echo "Running the Exploratory and Data processing..."
rm -rf output-explr
spark-submit --master local[4] --class "com.test.spark.MovieLensDataProc"  target/spark-data-0.1-SNAPSHOT-jar-with-dependencies.jar output-explr ratings.dat movies.dat >target/out.txt 2>target/err.txt 
echo "Completed the exploratory job successfully!"
echo "Explore output-explr directory for the generated output."

echo "Running Dimensionality reduction..."
rm -rf output-pca
spark-submit --master local[4] --class "com.test.spark.PCAUtil"  target/spark-data-0.1-SNAPSHOT-jar-with-dependencies.jar output-pca output-explr/user-genre-pref-scores 4 15 >target/out.txt 2>target/err.txt 
echo "Completed the PCA job successfully!"
echo "Explore output-pca directory for the generated output."


echo "Running Clustering..."
rm -rf output-pca-clustering
spark-submit --master local[4] --class "com.test.spark.PCAClusteringUtil" target/spark-data-0.1-SNAPSHOT-jar-with-dependencies.jar output-pca-clustering output-explr/user-genre-pref-scores output-pca/PCA-8/ 100 25 50 >target/out.txt 2>target/err.txt
echo "Completed Clustering successfully!"
echo "Explore output-pca-clustering for the generated output."
