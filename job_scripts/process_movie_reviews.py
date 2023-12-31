from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.functions import col, when, lit
from datetime import datetime


DATA_SOURCE_PATH = 'gs://de-captone-poject-bucket/dataset/movie_review.csv' # location of csv data file
DATA_OUTPUT_PATH = 'gs://de-captone-poject-bucket/staging_area' # output files saving location

def func_run():
    # Initialize a Spark session
    spark = SparkSession.builder.appName("MovieReviewsProcessing").getOrCreate()

    # Load the CSV file from Google Cloud Storage
    movie_reviews_df = spark.read.csv(DATA_SOURCE_PATH, header=True)

    # Data processing steps
    tokenizer = Tokenizer(inputCol="review_str", outputCol="review_token")
    movie_reviews_df = tokenizer.transform(movie_reviews_df)

    remover = StopWordsRemover(inputCol="review_token", outputCol="filtered_review")
    movie_reviews_df = remover.transform(movie_reviews_df)

    # Add a timestamp column
    movie_reviews_df = movie_reviews_df.withColumn("insert_date", lit(datetime.now()))

    # Define a UDF to convert a boolean column to an integer
    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType
    boolean_to_integer = udf(lambda x: 1 if x else 0, IntegerType())
    movie_reviews_df = movie_reviews_df.withColumn("positive_review", when(col("review_str").contains("good"), True).otherwise(False))
    movie_reviews_df = movie_reviews_df.withColumn("is_positive", boolean_to_integer(col("positive_review")))
    movie_reviews_df = movie_reviews_df.withColumnRenamed('id_review', 'review_id')
    # Select and rename the desired columns
    processed_df = movie_reviews_df.select("cid", "is_positive","review_id", "insert_date")

    # Save the results to a new file in GCS
    processed_df.write.csv(DATA_OUTPUT_PATH, header=True, mode="overwrite")

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    func_run()