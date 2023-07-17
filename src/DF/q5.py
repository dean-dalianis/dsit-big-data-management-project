import sys
import time

from pyspark.sql import SparkSession, types
from pyspark.sql.functions import avg

FILES_DIR_PATH = 'hdfs://master:9000/home/user/files/'

if __name__ == '__main__':
    start_time = time.time()

    spark_session = SparkSession.builder.appName('AverageMovieRevenue').getOrCreate()

    if len(sys.argv) > 1 and sys.argv[1] == '--parquet':
        use_parquet = True
    else:
        use_parquet = False

    if use_parquet:
        movies_df = spark_session.read.parquet(FILES_DIR_PATH + 'movies.parquet')
    else:
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

        movies_df = spark_session.read.csv(FILES_DIR_PATH + 'movies.csv', schema=schema, header=True)

    # Filter out non-valid rows
    movies_df = movies_df.filter((movies_df['release_year'] > 0) & (movies_df['revenue'] > 0))

    # Calculate the average revenue for each year
    avg_revenue_by_year = movies_df.groupBy('release_year').agg(avg('revenue').alias('avg_revenue'))

    # Sort by year
    sorted_avg_revenue = avg_revenue_by_year.sort('release_year')

    print('Average Revenue by Year:')
    for row in sorted_avg_revenue.collect():
        print('Year: {}, Average Revenue: ${:,.2f}'.format(row['release_year'], row['avg_revenue']))

    spark_session.stop()

    end_time = time.time()
    print('Execution Time: {:.2f} seconds'.format(end_time - start_time))
