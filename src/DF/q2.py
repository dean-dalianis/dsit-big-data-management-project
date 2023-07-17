import sys
import time

from pyspark.sql import SparkSession, types
from pyspark.sql.functions import col

FILES_DIR_PATH = 'hdfs://master:9000/home/user/files/'

if __name__ == '__main__':
    start_time = time.time()

    spark_session = SparkSession.builder.appName('MovieRating').getOrCreate()

    if len(sys.argv) > 1 and sys.argv[1] == '--parquet':
        use_parquet = True
    else:
        use_parquet = False

    if use_parquet:
        moviesDataFrame = spark_session.read.parquet(FILES_DIR_PATH + 'movies.parquet')
        ratingsDataFrame = spark_session.read.parquet(FILES_DIR_PATH + 'ratings.parquet')
    else:
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

        moviesDataFrame = spark_session.read.csv(FILES_DIR_PATH + 'movies.csv', schema=movies_schema)
        ratingsDataFrame = spark_session.read.csv(FILES_DIR_PATH + 'ratings.csv', schema=ratings_schema)

    # Filter movies for 'Cesare deve morire'
    cesare_movie = moviesDataFrame.filter(col('name') == 'Cesare deve morire')

    # Get movie ID for 'Cesare deve morire'
    cesare_movie_id = cesare_movie.select('movie_id').collect()[0][0]

    # Filter ratings for 'Cesare deve morire'
    cesare_ratings = ratingsDataFrame.filter(col('movie_id') == cesare_movie_id)

    # Count number of ratings and calculate average rating
    num_ratings = cesare_ratings.count()
    avg_rating = cesare_ratings.select('rating').groupBy().avg().collect()[0][0]

    print('Movie ID for "Cesare deve morire": {}'.format(cesare_movie_id))
    print('Number of ratings for "Cesare deve morire": {}'.format(num_ratings))
    print('Average rating for "Cesare deve morire": {:.2f}'.format(avg_rating))

    spark_session.stop()

    end_time = time.time()
    print('Execution Time: {:.2f} seconds'.format(end_time - start_time))
