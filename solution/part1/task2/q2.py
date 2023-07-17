import time

from pyspark import SparkConf, SparkContext


def parseInputMovies(line):
    fields = line.split(',')
    movie_id = int(fields[0])
    name = fields[1]
    return movie_id, name


def parseInputRatings(line):
    fields = line.split(',')
    movie_id = int(fields[1])
    rating = float(fields[2])
    return movie_id, rating


if __name__ == "__main__":
    start_time = time.time()

    conf = SparkConf().setAppName('MovieRating')
    sc = SparkContext(conf=conf)

    # Load movies data
    movies_data = sc.textFile('hdfs:///user/maria_dev/files/movies.csv')
    movies = movies_data.map(parseInputMovies)

    # Find movie id for "Cesare deve morire"
    cesare_movie_id = movies.filter(lambda movie: movie[1] == 'Cesare deve morire').collect()[0][0]
    print("Movie ID for 'Cesare deve morire':", cesare_movie_id)

    # Load ratings data
    ratings_data = sc.textFile('hdfs:///user/maria_dev/files/ratings.csv')
    ratings = ratings_data.map(parseInputRatings)

    # Filter ratings for "Cesare deve morire"
    cesare_ratings = ratings.filter(lambda movie: movie[0] == cesare_movie_id)

    # Compute number of ratings and average rating
    num_ratings = cesare_ratings.count()
    avg_rating = cesare_ratings.map(lambda movie: movie[1]).mean()

    print("Number of ratings for 'Cesare deve morire'", num_ratings)
    print("Average rating for 'Cesare deve morire'", avg_rating)

    # Stop the SparkContext
    sc.stop()

    end_time = time.time()
    print('Execution Time', end_time - start_time)
