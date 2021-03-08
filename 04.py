#
# Copyright 2021,  SenX S.A.S.
#

from pyspark.sql.types import *

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

spark = SparkSession.builder.appName("04").getOrCreate()
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
df.show(n=10,truncate=True)
