# SparkLearning

# WSL Installation
install pyspark option 3 https://spark.apache.org/downloads.html
* download spark from 
``` bash
wget https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
tar xzf spark-3.5.1-bin-hadoop3.tgz
rm spark-3.5.1-bin-hadoop3.tgz
```

install java
``` 
wget https://download.oracle.com/java/17/latest/jdk-17_linux-x64_bin.tar.gz
tar xzfv jdk-17_linux-x64_bin.tar.gz
```

nano ~/.bashrc and add to bottom of ~/.bashrc
``` 
export SPARK_HOME="/mnt/c/Users/Admin/Desktop/gitclones/spark/spark-3.5.1-bin-hadoop3"
export PATH="${SPARK_HOME}/bin:${PATH}"

export JAVA_HOME="/mnt/c/Users/Admin/Desktop/gitclones/spark/jdk-17.0.11"
export PATH="${JAVA_HOME}/bin:${PATH}"

export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"
```

# PySpark basics

Create spark session
``` python
import pyspark
from pyspark.sql import SparkSession, types
spark = SparkSession.builder \
        .master("local[*]") \
        .appName('test') \
        .getOrCreate()
```

Define schema (optional)
``` python
schema = types.StructType([
    types.StructField('hvfhs_license_num', types.StringType(), True), 
    types.StructField('dispatching_base_num', types.StringType(), True), 
    types.StructField('wav_match_flag', types.StringType(), True)]
)
```

Read parquet file
``` python
df = spark.read \
    .option("header","true") \
    .schema(schema) \
    .parquet("fhvhv_tripdata_2021-01.parquet")
```

Select and filter
``` python
df.select('pickup_datetime','dropoff_datetime','PULocationID','DOLocationID') \
    .filter(df.hvfhs_license_num=='HV0003')
```

create new columns with functions
``` python
from pyspark.sql import functions as F
df \
    .withColumn('pickup_Date',F.to_date(df.pickup_datetime)) \
    .withColumn('dropoff_Date',F.to_date(df.dropoff_datetime)) \
```

user defined functions
``` python
def crazy_stuff(base_num):
    num = int(base_num[1:])
    if num % 7 == 0:
        return f's/{num:03x}'
    else:
        return f'e/{num:03x}'
crazy_stuff_udf = F.udf(crazy_stuff, returnType=types.StringType())
df \
    .withColumn('crazy',crazy_stuff_udf(df.dispatching_base_num)) \
    .select('crazy','dispatching_base_num') \
    .show()
```

use sql
``` python
trips_data.createOrReplaceTempView('trips_data')
spark.sql("""
SELECT service_type,count(1) 
FROM trips_data
group by service_type

""").show()
```

Connect to GCS bucket
* download jar file from https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage
* download credentials with storage admin
* upload files to GCS bucket using gsutil
``` 
import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

credentials_location = '/mnt/c/Users/Admin/.gcp/newyorktaxi.json'
bucket = 'newyorktaxi-423104-bucket'

conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('test') \
    .set('spark.jars', "../bin/gcs-connector-hadoop3-2.2.22.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable","true") \
    .set("spark.hadoop.google.cloud.auth.service.json.keyfile",credentials_location)

sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")
```

Create master and workers
``` bash
# master
bash ./sbin/start-master.sh

# worker
bash ./sbin/start-master.sh spark://DESKTOP-RJFPN4L.:7077

# submitting spark jobs
spark-submit --master="spark://DESKTOP-RJFPN4L.:7077" 05_spark_cluster.py

# stop master and worker
bash ./sbin/stop-worker.sh
bash ./sbin/stop-master.sh
```

# Actions vs transformations
Transformations - lazy - selecting, filtering, joins, groupby
Actions - eager - show, head, write

# Spark cluster
* Driver (submits the spark job)
* Master (coordinator)
* Executors (does the work)

# Groupby reshuffling
* First stage create intermediate results
* second stage shuffle and create final results

# Join reshuffle (merge-sort join)
* First stage reshuffles so the same composite keys are together

# Broadcast
* When one table is much smaller than the other, its sent to each executor

# Map Reduce
* Map transforms rows into (key,value) pairs
* Map -> key-value -> shuffle -> reduce
* Reduce output based on key
* Assuming distributed file system