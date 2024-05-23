from pyspark.sql import SparkSession, types

# spark = SparkSession.builder \
#         .master("spark://DESKTOP-RJFPN4L.:7077") \
#         .appName('cluster') \
#         .getOrCreate()

spark = SparkSession.builder \
        .appName('cluster') \
        .getOrCreate()

green = spark.read.parquet('data/pq/green/*/*/')
green.repartition(12).write.mode('overwrite').parquet('data/report/')

#spark-submit --master="spark://DESKTOP-RJFPN4L.:7077" 05_spark_cluster.py