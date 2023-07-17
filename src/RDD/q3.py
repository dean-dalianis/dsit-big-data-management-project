import time

from pyspark import SparkConf, SparkContext

FILES_DIR_PATH = 'hdfs://master:9000/home/user/files/'


def parse_input_movies(line):
    fields = line.split(',')
    date = int(fields[3]) if fields[3].isdigit() else -1
    production_cost = int(fields[5])
    revenue = int(fields[6])

    if date == 1995 and production_cost != 0 and revenue != 0:
        movie_id = int(fields[0])
        name = fields[1]
        return movie_id, name, revenue
    else:
        return None


def parse_input_genres(line):
    fields = line.split(',')
    movie_id = int(fields[0])
    genre = fields[1]
    return movie_id, genre


if __name__ == '__main__':
    start_time = time.time()

    conf = SparkConf().setAppName('BestAnimationMovie')
    spark_context = SparkContext(conf=conf)

    movies_data = spark_context.textFile(FILES_DIR_PATH + 'movies.csv')
    movies = movies_data.map(parse_input_movies).filter(lambda movie: movie is not None)

    genres_data = spark_context.textFile(FILES_DIR_PATH + 'movie_genres.csv')
    genres = genres_data.map(parse_input_genres).filter(lambda genre: genre[1] == 'Animation')

    # Join movies and genres RDDs
    movies_with_animation = movies.join(genres).map(lambda movie: movie[1])

    # Find the movie with the highest revenue
    best_movie = movies_with_animation.max(lambda movie: movie[0][2])

    print(u'Best Animation Movie of 1995 in terms of revenue: {}'.format(best_movie[0]).encode('utf-8'))

    spark_context.stop()

    end_time = time.time()
    print('Execution Time: {:.2f} seconds'.format(end_time - start_time))
