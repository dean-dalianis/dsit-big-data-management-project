import sys
import time

from pyspark.sql import SparkSession, types
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

FILES_DIR_PATH = 'hdfs://master:9000/home/user/files/'

if __name__ == '__main__':
    start_time = time.time()

    spark_session = SparkSession.builder.appName('BestComedyMovies').getOrCreate()

    if len(sys.argv) > 1 and sys.argv[1] == '--parquet':
        use_parquet = True
    else:
        use_parquet = False

    if use_parquet:
        movies_df = spark_session.read.parquet(FILES_DIR_PATH + 'movies.parquet')
        genres_df = spark_session.read.parquet(FILES_DIR_PATH + 'movie_genres.parquet')
    else:
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

        genres_schema = types.StructType([
            types.StructField('movie_id', types.IntegerType()),
            types.StructField('genre', types.StringType()),
        ])

        movies_df = spark_session.read.csv(FILES_DIR_PATH + 'movies.csv', schema=movies_schema, header=True)
        genres_df = spark_session.read.csv(FILES_DIR_PATH + 'movie_genres.csv', schema=genres_schema, header=True)

    # Filter out non-valid rows
    movies_df = movies_df.filter((movies_df['release_year'] > 1995) &
                                 (movies_df['production_cost'] != 0) &
                                 (movies_df['revenue'] != 0))

    # Filter for comedy genre
    comedy_movie_ids = genres_df.filter(genres_df['genre'] == 'Comedy').select('movie_id').rdd.flatMap(
        lambda x: x).collect()

    # Filter movies dataframe for only comedy movies
    movies_with_comedy = movies_df.filter(movies_df['movie_id'].isin(comedy_movie_ids))

    # Define window for finding the most popular movie each year
    window = Window.partitionBy(movies_with_comedy['release_year']).orderBy(movies_with_comedy['popularity'].desc())

    # Add a row number for each window
    movies_with_comedy = movies_with_comedy.withColumn('rank', row_number().over(window))

    # Filter for the most popular movie each year
    best_movies_by_year = movies_with_comedy.filter(col('rank') == 1).drop('rank')

    sorted_movies = best_movies_by_year.sort('release_year')

    print('Best Comedy Movies by Year:')
    for row in sorted_movies.collect():
        print(u'Year: {}, Movie: {}'.format(row['release_year'], row['name']).encode('utf-8'))

    spark_session.stop()

    end_time = time.time()
    print('Execution Time: {:.2f} seconds'.format(end_time - start_time))
