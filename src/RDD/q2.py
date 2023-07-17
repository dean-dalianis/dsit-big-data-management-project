import time

from pyspark import SparkConf, SparkContext

FILES_DIR_PATH = 'hdfs://master:9000/home/user/files/'


def parse_input_movies(line):
    fields = line.split(',')
    movie_id = int(fields[0])
    name = fields[1]
    return movie_id, name


def parse_input_ratings(line):
    fields = line.split(',')
    movie_id = int(fields[1])
    rating = float(fields[2])
    return movie_id, rating


if __name__ == '__main__':
    start_time = time.time()

    conf = SparkConf().setAppName('MovieRating')
    spark_context = SparkContext(conf=conf)

    movies_data = spark_context.textFile(FILES_DIR_PATH + 'movies.csv')
    movies = movies_data.map(parse_input_movies)

    ratings_data = spark_context.textFile(FILES_DIR_PATH + 'ratings.csv')
    ratings = ratings_data.map(parse_input_ratings)

    # Find movie id for 'Cesare deve morire'
    cesare_movie_id = movies.filter(lambda movie: movie[1] == 'Cesare deve morire').first()[0]

    # Filter ratings for 'Cesare deve morire'
    cesare_ratings = ratings.filter(lambda movie: movie[0] == cesare_movie_id)

    # Compute number of ratings and average rating
    num_ratings = cesare_ratings.count()
    avg_rating = cesare_ratings.map(lambda movie: movie[1]).mean()

    print('Movie ID for "Cesare deve morire": {}'.format(cesare_movie_id))
    print('Number of ratings for "Cesare deve morire": {}'.format(num_ratings))
    print('Average rating for "Cesare deve morire": {:.2f}'.format(avg_rating))

    spark_context.stop()

    end_time = time.time()
    print('Execution Time: {:.2f} seconds'.format(end_time - start_time))
