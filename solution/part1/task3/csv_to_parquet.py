from pyspark.sql import SparkSession, types

# Start a SparkSession
spark = SparkSession.builder.appName('CSVtoParquet').getOrCreate()

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

# Define the schema for ratings
ratings_schema = types.StructType([
    types.StructField('user_id', types.IntegerType()),
    types.StructField('movie_id', types.IntegerType()),
    types.StructField('rating', types.FloatType()),
    types.StructField('timestamp', types.TimestampType()),
])

# Define the schema for movie genres
genres_schema = types.StructType([
    types.StructField('movie_id', types.IntegerType()),
    types.StructField('genre', types.StringType()),
])

# Read CSV files into DataFrames
movies_df = spark.read.csv('hdfs:///user/maria_dev/files/movies.csv', schema=movies_schema, header=True)
ratings_df = spark.read.csv('hdfs:///user/maria_dev/files/ratings.csv', schema=ratings_schema, header=True)
genres_df = spark.read.csv('hdfs:///user/maria_dev/files/movie_genres.csv', schema=genres_schema, header=True)

# Write DataFrames to Parquet
movies_df.repartition(1).write.parquet('hdfs:///user/maria_dev/files/movies.parquet')
ratings_df.repartition(1).write.parquet('hdfs:///user/maria_dev/files/ratings.parquet')
genres_df.repartition(1).write.parquet('hdfs:///user/maria_dev/files/movie_genres.parquet')

# Stop the SparkSession
spark.stop()
