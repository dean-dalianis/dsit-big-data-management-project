"""
For every year after 1995 print the difference between the money spent to create the movie and the
revenue of the movie (revenue – production cost).
INFO: For this query you will need to filter out the entries that have zero values for the columns revenue
and/or cost
"""

from pyspark import SparkConf, SparkContext


def loadMovieNames():
    movieNames = {}
    # this uses the u.item file in the ml-100k folder of the local filesystem
    with open("ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames


# field[1] = movieID, field[2] = rating
def parseInput(line):
    fields = line.split()
    return (int(fields[1]), (float(fields[2]), 1.0))


if __name__ == "__main__":
    # Create a SparkConf object
    conf = SparkConf().setAppName("WorstMovies")
    sc = SparkContext(conf=conf)

    # Load up our movie ID -> name python dictionary
    movieNames = loadMovieNames()

    # Load up the raw u.data file in an RDD object
    lines = sc.textFile("hdfs:///user/maria_dev/ml-100k/u.data")

    # Convert to (movieID, (rating, 1.0)) RDD
    movieRatings = lines.map(parseInput)

    # Reduce to (movieID, (sumOfRatings, totalRatings)) RDD
    # Uses reduceByKey to aggregate rating tuples for each movie by summing their ratings and counts.
    # The reduceByKey function operates by applying the provided function to pairs of values associated with the same key.
    ratingTotalsAndCount = movieRatings.reduceByKey(
        lambda movie1, movie2: (movie1[0] + movie2[0], movie1[1] + movie2[1]))

    # Map to (movieID, averageRating) RDD
    averageRatings = ratingTotalsAndCount.mapValues(lambda totalAndCount: totalAndCount[0] / totalAndCount[1])

    # Sort by average rating
    sortedMovies = averageRatings.sortBy(lambda ratingTuple: ratingTuple[1])

    # Take the top 10 results
    results = sortedMovies.take(10)

    # Print them out
    for result in results:
        print(movieNames[result[0]], result[1])