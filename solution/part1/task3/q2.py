import time
import sys
from pyspark.sql import SparkSession, types
from pyspark.sql.functions import col

if __name__ == "__main__":
    start_time = time.time()

    spark = SparkSession.builder.appName('MovieRating').getOrCreate()

    # Check for command line arguments
    if len(sys.argv) > 1 and sys.argv[1] == "--parquet":
        use_parquet = True
    else:
        use_parquet = False

    if use_parquet:
        # Read Parquet data into a DataFrame
        moviesDataFrame = spark.read.parquet('hdfs:///user/maria_dev/files/movies.parquet')
        ratingsDataFrame = spark.read.parquet('hdfs:///user/maria_dev/files/ratings.parquet')
    else:
        # Define the schemas
        movies_schema = types.StructType([
            types.StructField('movie_id', types.IntegerType()),
            types.StructField('name', types.StringType()),
            types.StructField('category', types.StringType()),
            types.StructField('release_year', types.IntegerType()),
            types.StructField('rating', types.FloatType()),
            types.StructField('production_cost', types.IntegerType()),
            types.StructField('revenue', types.IntegerType()),
        ])

        ratings_schema = types.StructType([
            types.StructField('user_id', types.IntegerType(), True),
            types.StructField('movie_id', types.IntegerType(), True),
            types.StructField('rating', types.FloatType(), True),
            types.StructField('timestamp', types.TimestampType(), True),
        ])

        # Read CSV data into a DataFrame
        moviesDataFrame = spark.read.csv('hdfs:///user/maria_dev/files/movies.csv', schema=movies_schema)
        ratingsDataFrame = spark.read.csv('hdfs:///user/maria_dev/files/ratings.csv', schema=ratings_schema)

    # Filter movies for "Cesare deve morire"
    cesare_movie = moviesDataFrame.filter(col('name') == 'Cesare deve morire')

    # Get movie ID for "Cesare deve morire"
    cesare_movie_id = cesare_movie.select('movie_id').collect()[0][0]
    print("Movie ID for 'Cesare deve morire'", cesare_movie_id)

    # Filter ratings for "Cesare deve morire"
    cesare_ratings = ratingsDataFrame.filter(col('movie_id') == cesare_movie_id)

    # Count number of ratings and calculate average rating
    num_ratings = cesare_ratings.count()
    avg_rating = cesare_ratings.select('rating').groupBy().avg().collect()[0][0]

    print("Number of ratings for 'Cesare deve morire'", num_ratings)
    print("Average rating for 'Cesare deve morire'", avg_rating)

    # Stop the SparkSession
    spark.stop()

    end_time = time.time()
    print('Execution Time', end_time - start_time)
