import time
from pyspark import SparkConf, SparkContext

def parseInputMovies(line):
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

def parseInputGenres(line):
    fields = line.split(',')
    movie_id = int(fields[0])
    genre = fields[1]
    return movie_id, genre

if __name__ == "__main__":
    start_time = time.time()

    conf = SparkConf().setAppName('BestComedyMovies')
    sc = SparkContext(conf=conf)

    # Load movies data
    movies_data = sc.textFile('hdfs:///user/maria_dev/files/movies.csv')
    movies = movies_data.map(parseInputMovies).filter(lambda movie: movie is not None)

    # Load genres data
    genres_data = sc.textFile('hdfs:///user/maria_dev/files/movie_genres.csv')
    genres = genres_data.map(parseInputGenres).filter(lambda genre: genre[1] == 'Comedy')

    # Join movies and genres RDDs using movie_id as the key
    movies_with_comedy = movies.leftOuterJoin(genres).filter(lambda x: x[1][1] is not None).map(lambda x: (x[0], x[1][0][0], x[1][0][1], x[1][0][2]))

    print(movies_with_comedy.take(5))

    # Group movies by year
    grouped_movies = movies_with_comedy.groupBy(lambda movie: movie[2])

    # Find the movie with the highest popularity for each year
    best_movies = grouped_movies.map(lambda movie: (movie[0], max(movie[1], key=lambda m: m[3])))

    # Sort movies by year
    sorted_movies = best_movies.sortByKey()

    # Print the best comedy movie for each year
    print('Best Comedy Movies by Year:')
    for year, movie in sorted_movies.collect():
        print(year, movie[1])

    # Stop the SparkContext
    sc.stop()

    end_time = time.time()
    print('Execution Time', end_time - start_time)
