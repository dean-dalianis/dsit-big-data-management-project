import time
import sys
from pyspark.sql import SparkSession, types
from pyspark.sql.functions import col

if __name__ == "__main__":
    start_time = time.time()

    spark = SparkSession.builder.appName('BestAnimationMovie').getOrCreate()

    # Check for command line arguments
    if len(sys.argv) > 1 and sys.argv[1] == "--parquet":
        use_parquet = True
    else:
        use_parquet = False

    if use_parquet:
        # Read Parquet data into a DataFrame
        movies_df = spark.read.parquet('hdfs:///user/maria_dev/files/movies.parquet')
        genres_df = spark.read.parquet('hdfs:///user/maria_dev/files/movie_genres.parquet')
    else:
        # Define the schema for movies
        movies_schema = types.StructType([
            types.StructField('movie_id', types.IntegerType()),
            types.StructField('name', types.StringType()),
            types.StructField('description', types.StringType()),
            types.StructField('release_year', types.IntegerType()),
            types.StructField('duration', types.IntegerType()),
            types.StructField('production_cost', types.IntegerType()),
            types.StructField('revenue', types.IntegerType()),
            types.StructField('popularity', types.FloatType()),
        ])

        # Define the schema for genres
        genres_schema = types.StructType([
            types.StructField('movie_id', types.IntegerType()),
            types.StructField('genre', types.StringType()),
        ])

        # Read CSV data into a DataFrame
        movies_df = spark.read.csv('hdfs:///user/maria_dev/files/movies.csv', schema=movies_schema, header=True)
        genres_df = spark.read.csv('hdfs:///user/maria_dev/files/movie_genres.csv', schema=genres_schema, header=True)

    # Filter out non-valid rows
    movies_df = movies_df.filter((movies_df['release_year'] == 1995) &
                                 (movies_df['production_cost'] != 0) &
                                 (movies_df['revenue'] != 0))

    # Filter for animation genre
    genres_df = genres_df.filter(genres_df['genre'] == 'Animation')

    # Join movies and genres DataFrames
    movies_with_animation = movies_df.join(genres_df, movies_df.movie_id == genres_df.movie_id)

    # Find the movie with the highest revenue
    best_movie = movies_with_animation.orderBy(movies_with_animation['revenue'].desc()).first()

    print('Best Animation Movie of 1995 in terms of revenue', best_movie['name'])

    # Stop the session
    spark.stop()

    end_time = time.time()
    print('Execution Time', end_time - start_time)
