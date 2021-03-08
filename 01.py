#
# Copyright 2021,  SenX S.A.S.
#

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

spark = SparkSession.builder.appName("01").getOrCreate()
sc = spark.sparkContext

sqlContext = SQLContext(sc)

##
## Configuration used to fetch data from a Warp 10 instance
##

conf = {}

##
## This defines your Warp 10 instance
##

# This is the host and port of your Warp 10 instance
conf['warp10.fetcher.fallbacks'] = '127.0.0.1'
conf['warp10.fetcher.port'] = '8080'

conf['warp10.fetcher.path'] = '/api/v0/sfetch'
conf['warp10.fetcher.fallbacksonly'] = 'true'
conf['warp10.fetcher.protocol'] = 'http'
conf['http.header.now'] = 'X-Warp10-Now'
conf['http.header.timespan'] = 'X-Warp10-Timespan'

##
## Ensure your Warp 10 instance is configured with
## standalone.splits.enable = true
##
conf['warp10.splits.endpoint'] = 'http://127.0.0.1:8080/api/v0/splits'

# Token to use for fetching data
conf['warp10.splits.token'] = 'READ'
conf['warp10.splits.selector'] = '~.*{}'
# We fetch a single data point from the GTS, this could be an actual timespan if it were a positive value
conf['warp10.fetch.now'] = '2000000000000000'
conf['warp10.fetch.timespan'] = '-1'

# Maximum number of splits to generate
conf['warp10.max.splits'] = '2'

conf['warp10.http.connect.timeout'] = '60000'
conf['warp10.http.read.timeout'] = '60000'

##
## The Warp10InputFormat will return tuples (pairs) with an id (16 bytes as an hexadecimal STRING) of GTS and a wrapper containing a chunk of the said GTS.
##

rdd = sc.newAPIHadoopRDD('io.warp10.hadoop.Warp10InputFormat', 'org.apache.hadoop.io.Text', 'org.apache.hadoop.io.BytesWritable', conf=conf)
df = rdd.toDF()

df.show(n=10,truncate=True)
