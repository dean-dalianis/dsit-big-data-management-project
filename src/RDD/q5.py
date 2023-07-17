import time

from pyspark import SparkConf, SparkContext

FILES_DIR_PATH = 'hdfs://master:9000/home/user/files/'


def parse_input_movies(line):
    fields = line.split(',')
    date = int(fields[3]) if fields[3].isdigit() else -1
    revenue = int(fields[6])

    if date > 0 and revenue > 0:
        # Return a tuple with the date as the key and a tuple containing the revenue and count as the value.
        # The count represents the number of valid movie entries for a particular date.
        return date, (revenue, 1)
    else:
        return None


def sum_revenues_counts(value1, value2):
    revenue1, count1 = value1
    revenue2, count2 = value2

    # Sum the revenues from value1 and value2
    total_revenue = revenue1 + revenue2

    # Sum the counts from value1 and value2
    total_count = count1 + count2

    # Return a tuple with the summed revenue and count
    return total_revenue, total_count


if __name__ == '__main__':
    start_time = time.time()

    conf = SparkConf().setAppName('AverageMovieRevenue')
    spark_context = SparkContext(conf=conf)

    movies_data = spark_context.textFile(FILES_DIR_PATH + 'movies.csv')
    movies = movies_data.map(parse_input_movies).filter(lambda movie: movie is not None)

    # Calculate the sum and count of revenues for each year
    # The reduceByKey transformation aggregates the values of each key (year) using the sum_revenues_counts function.
    # It combines the revenue and count tuples for each year and applies the sum_revenues_counts function to merge them.
    # The result is a new RDD with each key (year) and its corresponding aggregated revenue and count tuple.
    revenues_counts_by_year = movies.reduceByKey(sum_revenues_counts)

    # Calculate the average revenue for each year
    average_revenue_by_year = revenues_counts_by_year.map(
        lambda year_revenue: (year_revenue[0], year_revenue[1][0] / year_revenue[1][1]))

    # Sort the average revenue by year
    sorted_avg_revenue = average_revenue_by_year.sortByKey()

    print('Average Revenue by Year:')
    for row in sorted_avg_revenue.collect():
        print('Year: {}, Average Revenue: ${:,.2f}'.format(row[0], row[1]))

    spark_context.stop()

    end_time = time.time()
    print('Execution Time: {:.2f} seconds'.format(end_time - start_time))
