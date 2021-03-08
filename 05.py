# Copyright 2021,  SenX S.A.S.
#

from pyspark.sql.types import *

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

spark = SparkSession.builder.appName("05").getOrCreate()
sc = spark.sparkContext

sqlContext = SQLContext(sc)

##
## Read observations from a CSV file with lines containing GTS,TS,VALUE
##

schema = StructType([ StructField('gts', StringType(), True), StructField('ts', LongType(), True), StructField('value', DoubleType(), True)])
df = spark.read.csv('04.csv', sep=r',', header=True, schema=schema)


##
## Generate a DF with a pair of values so we can group by key the underlying RDD
## Note, this uses a lambda and therefore will spawn a python interpreter to
## process the elements
##

rdd = df.rdd.map(lambda x: (x['gts'],x))
df = rdd.groupByKey().toDF()

##
## Now create a temp view so the DF can be processed by WarpScript code which will create GTS Wrappers, one per GTS
##

df.createOrReplaceTempView('GROUPED')
sqlContext.registerJavaFunction("ws", "io.warp10.spark.WarpScriptUDF2", "BINARY")

df = sqlContext.sql("SELECT ws('@04.mc2', _2) AS wrapper FROM GROUPED")

##
## The Warp10OutputFormat expects a RDD of pairs (None,byte array) where None is ignored and the byte array
## contains a wrapper (a GTS or GTS Encoder processed by WRAPRAW)
##

rdd = df.rdd.map(lambda x: (None,x['wrapper']))

##
## Store in Warp 10
##

conf = {}
conf['warp10.gzip'] = 'true'
conf['warp10.endpoint'] = 'http://127.0.0.1:8080/api/v0/update'
conf['warp10.token'] = 'WRITE'
conf['warp10.maxrate'] = '100000000.0'

rdd.saveAsNewAPIHadoopFile('/dev/null','io.warp10.hadoop.Warp10OutputFormat',keyClass='org.apache.hadoop.io.NullWritable',valueClass='org.apache.hadoop.io.Text',conf=conf)
