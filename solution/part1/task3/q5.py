import time
import sys
from pyspark.sql import SparkSession, types
from pyspark.sql.functions import col, avg

if __name__ == "__main__":
    start_time = time.time()

    spark = SparkSession.builder.appName('AverageMovieRevenue').getOrCreate()

    # Check for command line arguments
    if len(sys.argv) > 1 and sys.argv[1] == "--parquet":
        use_parquet = True
    else:
        use_parquet = False

    if use_parquet:
        # Read Parquet data into a DataFrame
        movies_df = spark.read.parquet('hdfs:///user/maria_dev/files/movies.parquet')
    else:
        # Define the schema for movies
        schema = types.StructType([
            types.StructField('movie_id', types.IntegerType()),
            types.StructField('name', types.StringType()),
            types.StructField('description', types.StringType()),
            types.StructField('release_year', types.IntegerType()),
            types.StructField('duration', types.IntegerType()),
            types.StructField('production_cost', types.IntegerType()),
            types.StructField('revenue', types.IntegerType()),
            types.StructField('popularity', types.FloatType()),
        ])

        # Read CSV data into a DataFrame
        movies_df = spark.read.csv('hdfs:///user/maria_dev/files/movies.csv', schema=schema, header=True)

    # Filter out non-valid rows
    movies_df = movies_df.filter((movies_df['release_year'] > 0) & (movies_df['revenue'] > 0))

    # Calculate the average revenue for each year
    avg_revenue_by_year = movies_df.groupBy('release_year').agg(avg('revenue').alias('avg_revenue'))

    # Sort by year
    sorted_avg_revenue = avg_revenue_by_year.sort('release_year')

    # Print the results
    sorted_avg_revenue.show()

    # Stop the session
    spark.stop()

    end_time = time.time()
    print('Execution Time', end_time - start_time)
