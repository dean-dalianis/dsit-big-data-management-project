import time

from pyspark import SparkConf, SparkContext

FILES_DIR_PATH = 'hdfs://master:9000/home/user/files/'


def parse_input_movies(line):
    fields = line.split(',')
    date = int(fields[3]) if fields[3].isdigit() else -1
    production_cost = int(fields[5])
    revenue = int(fields[6])

    if date > 1995 and production_cost != 0 and revenue != 0:
        movie_id = int(fields[0])
        name = fields[1]
        return movie_id, name, date, revenue - production_cost
    else:
        return None


if __name__ == '__main__':
    start_time = time.time()

    conf = SparkConf().setAppName('MovieProfit')
    spark_context = SparkContext(conf=conf)

    movies_data = spark_context.textFile(FILES_DIR_PATH + 'movies.csv')
    movies = movies_data.map(parse_input_movies).filter(lambda movie: movie is not None)

    # Sort by profit
    sortedMovies = movies.sortBy(lambda movie: movie[3], ascending=False)

    # collect the results for timing purposes
    collectedMovies = sortedMovies.collect()

    for row in sortedMovies.collect():
        print(u'Movie ID: {}, Movie Name: "{}", Release Year: {}, Profit: ${:,.2f}'.format(row[0], row[1], row[2],
                                                                                           row[3]).encode('utf-8'))

    spark_context.stop()

    end_time = time.time()
    print('Execution Time: {:.2f} seconds'.format(end_time - start_time))
