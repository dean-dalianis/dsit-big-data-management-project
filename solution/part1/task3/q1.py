import time
import sys
from pyspark.sql import SparkSession, types
from pyspark.sql.functions import col

if __name__ == "__main__":
    start_time = time.time()

    spark = SparkSession.builder.appName('MovieProfit').getOrCreate()

    # Check for command line arguments
    if len(sys.argv) > 1 and sys.argv[1] == "--parquet":
        use_parquet = True
    else:
        use_parquet = False

    if use_parquet:
        # Read Parquet data into a DataFrame
        moviesDataFrame = spark.read.parquet('hdfs:///user/maria_dev/files/movies.parquet')
    else:
        # Define the schema for CSV
        schema = types.StructType([
            types.StructField('movie_id', types.IntegerType()),
            types.StructField('name', types.StringType()),
            types.StructField('category', types.StringType()),
            types.StructField('release_year', types.IntegerType()),
            types.StructField('rating', types.FloatType()),
            types.StructField('production_cost', types.IntegerType()),
            types.StructField('revenue', types.IntegerType()),
        ])

        # Read CSV data into a DataFrame
        moviesDataFrame = spark.read.csv('hdfs:///user/maria_dev/files/movies.csv', schema=schema)

    # Filter out non-valid rows
    moviesDataFrame = moviesDataFrame.filter((moviesDataFrame['release_year'] > 1995) &
                                             (moviesDataFrame['production_cost'] != 0) &
                                             (moviesDataFrame['revenue'] != 0))

    # Calculate profit and add it as a new column
    moviesDataFrame = moviesDataFrame.withColumn('profit',
                                                 moviesDataFrame['revenue'] - moviesDataFrame['production_cost'])

    # Sort by profit
    sortedMovies = moviesDataFrame.sort(col('profit').desc())

    # collect the results for timing purposes
    collectedMovies = sortedMovies.collect()

    # Stop the session
    spark.stop()

    end_time = time.time()
    print('Execution Time', end_time - start_time)

    # Print the top 10 results
    print('Top 10 Movies in terms of profit:')
    topTen = collectedMovies[:10]
    for movie in topTen:
        print(movie[0], movie[1], movie[3], movie['profit'])
