# Spark Structured Streaming skeleton
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

spark = SparkSession.builder.appName('stream-job').getOrCreate()
df = spark.readStream.format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('subscribe', 'events') \
    .load()

# Assuming value is JSON
json_schema = 'id INT, value INT'
parsed = df.selectExpr('CAST(value AS STRING) as json').select(from_json(col('json'), json_schema).alias('data')).select('data.*')
agg = parsed.groupBy('id').count()

query = agg.writeStream.format('console').start()
query.awaitTermination()
