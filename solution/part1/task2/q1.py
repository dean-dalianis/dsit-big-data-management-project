import time

from pyspark import SparkConf, SparkContext


def parseInputMovies(line):
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


if __name__ == "__main__":
    start_time = time.time()

    conf = SparkConf().setAppName('MovieProfit')
    sc = SparkContext(conf=conf)

    # Load movies data
    movies_data = sc.textFile('hdfs:///user/maria_dev/files/movies.csv')
    movies = movies_data.map(parseInputMovies).filter(lambda movie: movie is not None)

    # Sort by profit
    sortedMovies = movies.sortBy(lambda movie: movie[3], ascending=False)

    # collect the results
    collectedMovies = sortedMovies.collect()

    # Stop the SparkContext
    sc.stop()

    end_time = time.time()
    print('Execution Time', end_time - start_time)

    # Print the top 10 results
    print('Top 10 Movies in terms of profit:')
    topTen = collectedMovies[:10]
    for movie in topTen:
        print(movie[0], movie[1], movie[2], movie[3])

