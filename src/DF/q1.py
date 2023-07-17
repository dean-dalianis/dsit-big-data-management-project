import sys
import time

from pyspark.sql import SparkSession, types
from pyspark.sql.functions import col

FILES_DIR_PATH = 'hdfs://master:9000/home/user/files/'

if __name__ == '__main__':
    start_time = time.time()

    spark_session = SparkSession.builder.appName('MovieProfit').getOrCreate()

    if len(sys.argv) > 1 and sys.argv[1] == '--parquet':
        use_parquet = True
    else:
        use_parquet = False

    if use_parquet:
        moviesDataFrame = spark_session.read.parquet(FILES_DIR_PATH + 'movies.parquet')
    else:
        schema = types.StructType([
            types.StructField('movie_id', types.IntegerType()),
            types.StructField('name', types.StringType()),
            types.StructField('category', types.StringType()),
            types.StructField('release_year', types.IntegerType()),
            types.StructField('rating', types.FloatType()),
            types.StructField('production_cost', types.IntegerType()),
            types.StructField('revenue', types.IntegerType()),
        ])

        moviesDataFrame = spark_session.read.csv(FILES_DIR_PATH + 'movies.csv', schema=schema)

    # Filter out non-valid rows
    moviesDataFrame = moviesDataFrame.filter((moviesDataFrame['release_year'] > 1995) &
                                             (moviesDataFrame['production_cost'] != 0) &
                                             (moviesDataFrame['revenue'] != 0))

    # Calculate profit and add it as a new column
    moviesDataFrame = moviesDataFrame.withColumn('profit',
                                                 moviesDataFrame['revenue'] - moviesDataFrame['production_cost'])

    # Sort by profit
    sortedMovies = moviesDataFrame.sort(col('profit').desc())

    for row in sortedMovies.collect():
        print(u'Movie ID: {}, Movie Name: "{}", Release Year: {}, Profit: ${:,.2f}'.format(row[0], row[1], row[3],
                                                                                           row['profit']).encode(
            'utf-8'))

    spark_session.stop()

    end_time = time.time()
    print('Execution Time: {:.2f} seconds'.format(end_time - start_time))
