import sys
import time

from pyspark.sql import SparkSession, types

FILES_DIR_PATH = 'hdfs://master:9000/home/user/files/'

if __name__ == '__main__':
    start_time = time.time()

    spark = SparkSession.builder.appName('BestAnimationMovie').getOrCreate()

    if len(sys.argv) > 1 and sys.argv[1] == '--parquet':
        use_parquet = True
    else:
        use_parquet = False

    if use_parquet:
        movies_df = spark.read.parquet(FILES_DIR_PATH + 'movies.parquet')
        genres_df = spark.read.parquet(FILES_DIR_PATH + 'movie_genres.parquet')
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

        movies_df = spark.read.csv(FILES_DIR_PATH + 'movies.csv', schema=movies_schema, header=True)
        genres_df = spark.read.csv(FILES_DIR_PATH + 'movie_genres.csv', schema=genres_schema, header=True)

    # Filter out non-valid rows
    movies_df = movies_df.filter((movies_df['release_year'] == 1995) &
                                 (movies_df['production_cost'] != 0) &
                                 (movies_df['revenue'] != 0))

    # Filter for animation genre
    animation_movie_ids = genres_df.filter(genres_df['genre'] == 'Animation').select('movie_id').rdd.flatMap(
        lambda x: x).collect()

    # Filter movies dataframe for only animation movies
    movies_with_animation = movies_df.filter(movies_df['movie_id'].isin(animation_movie_ids))

    # Find the movie with the highest revenue
    best_movie = movies_with_animation.orderBy(movies_with_animation['revenue'].desc()).first()

    print(u'Best Animation Movie of 1995 in terms of revenue: {}'.format(best_movie['name']).encode('utf-8'))

    spark.stop()

    end_time = time.time()
    print('Execution Time: {:.2f} seconds'.format(end_time - start_time))
