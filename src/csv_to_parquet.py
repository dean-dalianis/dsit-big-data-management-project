from pyspark.sql import SparkSession, types

FILES_DIR_PATH = 'hdfs://master:9000/home/user/files/'

spark_session = SparkSession.builder.appName('CSVtoParquet').getOrCreate()

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

ratings_schema = types.StructType([
    types.StructField('user_id', types.IntegerType()),
    types.StructField('movie_id', types.IntegerType()),
    types.StructField('rating', types.FloatType()),
    types.StructField('timestamp', types.TimestampType()),
])

genres_schema = types.StructType([
    types.StructField('movie_id', types.IntegerType()),
    types.StructField('genre', types.StringType()),
])

movies_df = spark_session.read.csv(FILES_DIR_PATH + 'movies.csv', schema=movies_schema, header=True)
ratings_df = spark_session.read.csv(FILES_DIR_PATH + 'ratings.csv', schema=ratings_schema, header=True)
genres_df = spark_session.read.csv(FILES_DIR_PATH + 'movie_genres.csv', schema=genres_schema, header=True)

movies_df.write.parquet(FILES_DIR_PATH + 'movies.parquet')
ratings_df.write.parquet(FILES_DIR_PATH + 'ratings.parquet')
genres_df.write.parquet(FILES_DIR_PATH + 'movie_genres.parquet')

spark_session.stop()
