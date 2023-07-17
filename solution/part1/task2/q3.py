import time

from pyspark import SparkConf, SparkContext


def parseInputMovies(line):
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

def parseInputGenres(line):
    fields = line.split(',')
    movie_id = int(fields[0])
    genre = fields[1]
    return movie_id, genre

if __name__ == "__main__":
    start_time = time.time()

    conf = SparkConf().setAppName('BestAnimationMovie')
    sc = SparkContext(conf=conf)

    # Load movies data
    movies_data = sc.textFile('hdfs:///user/maria_dev/files/movies.csv')
    movies = movies_data.map(parseInputMovies).filter(lambda movie: movie is not None)

    # Load genres data
    genres_data = sc.textFile('hdfs:///user/maria_dev/files/movie_genres.csv')
    genres = genres_data.map(parseInputGenres).filter(lambda genre: genre[1] == 'Animation')

    # Join movies and genres RDDs
    movies_with_animation = movies.join(genres).map(lambda movie: movie[1])

    # Find the movie with the highest revenue
    best_movie = movies_with_animation.max(lambda movie: movie[0][2])

    print('Best Animation Movie of 1995 in terms of revenue', best_movie[0])

    # Stop the SparkContext
    sc.stop()

    end_time = time.time()
    print('Execution Time', end_time - start_time)
