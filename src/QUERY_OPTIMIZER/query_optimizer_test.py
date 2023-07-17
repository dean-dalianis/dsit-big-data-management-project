import sys
import time

from pyspark.sql import SparkSession
import sys
import time

from pyspark.sql import SparkSession

FILES_DIR_PATH = 'hdfs://master:9000/home/user/files/'

disabled = sys.argv[1]
spark = SparkSession.builder.appName('query1-sql').getOrCreate()

if disabled == 'Y':
    spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '-1')
elif disabled == 'N':
    pass
else:
    raise Exception('This setting is not available.')

df = spark.read.format('parquet')
df1 = df.load(FILES_DIR_PATH + 'ratings.parquet')
df2 = df.load(FILES_DIR_PATH + 'movie_genres.parquet')
df1.registerTempTable('ratings')
df2.registerTempTable('movie_genres')

sqlString = 'SELECT * ' + \
            'FROM (SELECT * FROM movie_genres LIMIT 100) as g, ratings as r ' + \
            'WHERE r.movie_id = g.movie_id'

t1 = time.time()
spark.sql(sqlString).show()
t2 = time.time()
spark.sql(sqlString).explain()

print('Time with choosing join type %s is %.4f sec.' % ('enabled' if disabled == 'N' else 'disabled', t2 - t1))
