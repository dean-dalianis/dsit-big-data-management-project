import time

from pyspark import SparkConf, SparkContext

FILES_DIR_PATH = 'hdfs://master:9000/home/user/files/'


def parse_input_movies(line):
    fields = line.split(',')
    movie_id = int(fields[0])
    name = fields[1]
    date = int(fields[3]) if fields[3].isdigit() else -1
    popularity = float(fields[7])
    production_cost = int(fields[5])
    revenue = int(fields[6])

    if date > 1995 and production_cost != 0 and revenue != 0:
        return movie_id, (name, date, popularity)
    else:
        return None


def parse_input_genres(line):
    fields = line.split(',')
    movie_id = int(fields[0])
    genre = fields[1]
    return movie_id, genre


if __name__ == '__main__':
    start_time = time.time()

    conf = SparkConf().setAppName('BestComedyMovies')
    spark_context = SparkContext(conf=conf)

    movies_data = spark_context.textFile(FILES_DIR_PATH + 'movies.csv')
    movies = movies_data.map(parse_input_movies).filter(lambda movie: movie is not None)

    genres_data = spark_context.textFile(FILES_DIR_PATH + 'movie_genres.csv')
    genres = genres_data.map(parse_input_genres).filter(lambda genre: genre[1] == 'Comedy')

    # Join movies and genres RDDs using movie_id as the key
    movies_with_comedy = movies.leftOuterJoin(genres).filter(lambda x: x[1][1] is not None).map(
        lambda x: (x[0], x[1][0][0], x[1][0][1], x[1][0][2]))

    # Group movies by year
    grouped_movies = movies_with_comedy.groupBy(lambda movie: movie[2])

    # Find the movie with the highest popularity for each year
    best_movies = grouped_movies.map(lambda movie: (movie[0], max(movie[1], key=lambda m: m[3])))

    # Sort movies by year
    sorted_movies = best_movies.sortByKey()

    print('Best Comedy Movies by Year:')
    for row in sorted_movies.collect():
        print(u'Year: {}, Movie: {}'.format(row[0], row[1]).encode('utf-8'))

    spark_context.stop()

    end_time = time.time()
    print('Execution Time: {:.2f} seconds'.format(end_time - start_time))
