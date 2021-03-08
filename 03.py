#
# Copyright 2021,  SenX S.A.S.
#

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.functions import explode

spark = SparkSession.builder.appName("03").getOrCreate()
sc = spark.sparkContext

sqlContext = SQLContext(sc)

##
## Configuration used to fetch data from a Warp 10 instance
##

conf = {}
conf['warp10.fetcher.fallbacks'] = '127.0.0.1'
conf['warp10.fetcher.fallbacksonly'] = 'true'
conf['warp10.fetcher.protocol'] = 'http'
conf['http.header.now'] = 'X-Warp10-Now'
conf['http.header.timespan'] = 'X-Warp10-Timespan'
conf['warp10.fetcher.port'] = '8080'
conf['warp10.fetcher.path'] = '/api/v0/sfetch'
conf['warp10.splits.endpoint'] = 'http://127.0.0.1:8080/api/v0/splits'

# We fetch 10 data point from the GTS, this could be an actual timespan if it were a positive value
conf['warp10.fetch.timespan'] = '-10'

conf['warp10.http.connect.timeout'] = '60000'
conf['warp10.http.read.timeout'] = '60000'

# Maximum number of splits to generate
conf['warp10.max.splits'] = '2'

conf['warp10.splits.token'] = 'READ'
conf['warp10.splits.selector'] = '~.*{}'
conf['warp10.fetch.now'] = '2000000000000000'

##
## The Warp10InputFormat will return tuples (pairs) with an id (16 bytes as an hexadecimal STRING) of GTS and a wrapper containing a chunk of the said GTS.
##

rdd = sc.newAPIHadoopRDD('io.warp10.hadoop.Warp10InputFormat', 'org.apache.hadoop.io.Text', 'org.apache.hadoop.io.BytesWritable', conf=conf)
df = rdd.toDF()

##
## Now process each record with the code in 03.mc2 which will emit a list of Rows [ gts, ts, value ]
##

sqlContext.registerJavaFunction("ws", "io.warp10.spark.WarpScriptUDF2", "ARRAY< STRUCT<gts: STRING, ts: LONG,value: STRING> >")

# Create a temp view so the DF can be manipulated using SparlSQL
df.createOrReplaceTempView('VIEW')

# Since the WarpScript code emits an array of Rows, we must explode the array and then extract the elements from each Row to
# get a dataframe with the observations

df = sqlContext.sql("SELECT explode(ws('@03.mc2', _2)) AS row FROM VIEW")
df = df.select("row.gts","row.ts","row.value")

##
## df now contains rows of observations: gts,ts,value
##

df.show(n=10,truncate=True)
