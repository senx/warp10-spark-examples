There is no doubt time series data are becoming more and more important in many different verticals. The growing number of time series databases is a clue this trend is here to stay.


Different TSDBs target different use cases, some do monitoring, others do finance, yet others specialize in IoT. They all come with their own way of querying or manipulating the data. Some TSDBs can only be used as dumb data stores, relying on external tools for processing the data, while others provide more advanced analytics capabilities. Most TSDBs claim they are scalable, and to some extent this is true. But when it comes to processing or analyzing massive historical data sets, no TSDB actually works and they will all recommend the use of tools such as Spark. And this is when problems arise.


The main issue with the use of Spark on time series data is that time series are not a type of data that can be manipulated natively and that Spark lacks built-in functions to perform time series manipulation on its data frames. There have been some efforts in the past to make Spark time series aware, [spark-ts](https://github.com/sryza/spark-timeseries) was a package backed by Cloudera, it was created to bring time series forecasting to Spark, unfortunately it is no longer maintained. More recently, the hedge fund [Two Sigma](https://www.twosigma.com/) has presented the [Flint](https://www.twosigma.com/articles/introducing-flint-a-time-series-library-for-apache-spark/) library, an extension to PySpark which brings the notion of TimeSeriesDataFrame. This latter effort is very finance oriented and does not seem to be actively maintained since the last activity on [GitHub](https://github.com/twosigma/flint) dates back more than two years. This leaves the Spark ecosystem with little to no alternative other than crafting custom UDFs to work on time series data, [koalas](https://koalas.readthedocs.io/en/latest/) being the most recent initiative. The analytics approaches of most TSDBs cannot be used in Spark since in the vast majority of cases they are limited to working inside those very TSDBs.


But there is hope, the Warp 10 Time Series Platform has, since its very inception, separated its storage and analytics capabilities, making it possible to use the Warp 10 Analytics Engine features on data not residing in the Warp 10 Storage Engine. As part of this approach, the Warp 10 Analytics Engine has been integrated with Spark to augment it with the power of WarpScript and WarpLib. This means that any effort you may have put in crafting custom macros can be leveraged by using those pieces of code in your Spark jobs, no need to rewrite anything. And should you switch to Flink later, know that the same integration exists for it, thus increasing your ROI a little more.


# The philosophy of the Warp 10 Spark integration


The Spark framework is written in Scala and any Spark job is handled by a coordinated army of Java Virtual Machines. When using Spark via PySpark or SparkR, external interpreters for those languages must be spawned and data moved back and forth between the JVM where it natively resides and those external programs where processing happens, so even though Spark allows to scale data processing done in Python or R this is done in a very inefficient manner and in the cloud era we live in it could lead to extraneous costs since resources are not optimized. For a deep dive in the PySpark execution model you can read this [blog post](https://steadbytes.com/blog/pyspark-runtime-architecture/).


The Warp 10 Analytics Engine executes on the JVM, this means that its integration in Spark will avoid those costly data transfers, thus offering better performance and reduced resources. The integration is done via functions which can be leveraged in your Spark DAG (when working with the Java or Scala API) or called from SparkSQL statements. This latter possibility is the only one supported when working in PySpark, but the SparkSQL functions will still run inside the JVM.


When working with WarpScript or FLoWS, most of the time you manipulate Geo Time Series or GTS Encoders which were retrieved from a storage backend. The philosophy of the Spark integration is identical, load data from whatever source, create chunks of GTS or GTS Encoders, process those chunks in Spark transformations using WarpScript and pass the results to the rest of your job.


## In practice


The actual code of the Warp 10 Analytics Engine integration in Spark is simply an external package that can be loaded by Spark when you submit your job.


Adding the following options to [`spark-submit`](https://spark.apache.org/docs/latest/submitting-applications.html) is sufficient to make the Warp 10 Analytics Engine available to your job:


```
--packages io.warp10:warp10-spark:1.0.5 --repositories https://dl.bintray.com/senx/maven
```


The rest of this post will walk you through some examples. In order to keep the post small, the code is available on a github [repository](git clone https://github.com/senx/warp10-spark-examples) that you should clone locally in order to run the examples:


```
git clone https://github.com/senx/warp10-spark-examples.git
```


The examples are all in PySpark, but similar results could be obtained using the Scala or Java API. Please refer to the [`warp10-spark2`](https://github.com/senx/warp10-spark2) code.


# RDD, DataSets and DataFrames


Before we dive deeper into the use of the Warp 10 Analytics Engine in Spark, we need a quick reminder on the data structures used in Spark.


The foundation of Spark is the `RDD`, which stands for [Resilient Distributed Dataset](https://spark.apache.org/docs/latest/rdd-programming-guide.html#resilient-distributed-datasets-rdds). In early versions of Spark this was the only data structure available.


Spark 1.6 introduced an additional data structure, the [Dataset](https://spark.apache.org/docs/latest/sql-programming-guide.html) which can be manipulated with [SparkSQL](https://spark.apache.org/sql/). Datasets are backed by RDDs.


Lastly Spark introduced *DataFrames* which are *Datasets* of *Rows* which are themselves a collection of named columns. So DataFrames are therefore also backed by RDDs. And throughout Spark code it is very common to go back and forth between those data structures since some operations are only possible on some of them.


# Reading data from Warp 10


If you have data stored in the Warp 10 Storage Engine, the first thing you want to try out is to read those data from your instance. For this purpose, Warp 10 offers what is called a [Hadoop InputFormat](https://hadoop.apache.org/docs/current/api/org/apache/hadoop/mapreduce/InputFormat.html) named [`Warp10InputFormat`](https://github.com/senx/warp10-platform/blob/master/warp10/src/main/java/io/warp10/hadoop/Warp10InputFormat.java).


As all `InputFormat`s, the `Warp10InputFormat` will create *splits* (subsets of the overall data) which will then be read by parallel tasks in a Spark job. In order for your standalone Warp 10 instance to be able to create those splits, the following configuration must be added:


```
standalone.splits.enable = true
```


The `Warp10InputFormat` will return key/value pairs where the key is a `String` identifying a Geo Time Series and the value a byte array containing a [`wrapper`](https://warp10.io/doc/WRAPRAW) of a chunk of a Geo Time Series. As the length of a GTS may vary and be extremely large, there is no guarantee that a value will contain the entirety of the requested GTS. You will need to group chunks together to recreate the complete GTS if you need to work on it, and that may not always be possible if the GTS is really large, so working on [chunks](https://warp10.io/doc/CHUNK) is ususally better.


You can run the first example from the `warp10-spark-examples` to read data from your Warp 10 instance. You should set `SPARK_HOME` and `JAVA_HOME` in `runspark.sh` to values matching your environment.


Also inspect and modify the in file `01.py` so the example can find your Warp 10 instance.


When done issue the following command from the `warp10-spark-examples` directory:


```
sh runspark.sh 01.py
```


The result will depend on your own data but you can expect something like this:


```
+--------------------------------+--------------------------------------------------------------------------------+
|                              _1|                                                                              _2|
+--------------------------------+--------------------------------------------------------------------------------+
|00027d544a5f03c3aa1383f09c2d2d54|[1C 18 59 4E 61 6D 65 73 70 61 63 65 5F 64 65 66 61 75 6C 74 5F 74 61 62 6C 6...|
|0006567dc5cba213aa1383f09c2d2d54|[1C 18 5B 4E 61 6D 65 73 70 61 63 65 5F 64 65 66 61 75 6C 74 5F 74 61 62 6C 6...|
|0012c3a09e52e0bdaa1383f09c2d2d54|[1C 18 67 4E 61 6D 65 73 70 61 63 65 5F 64 65 66 61 75 6C 74 5F 74 61 62 6C 6...|
|0013ed39a4ebe998aa1383f09c2d2d54|[1C 18 69 4E 61 6D 65 73 70 61 63 65 5F 64 65 66 61 75 6C 74 5F 74 61 62 6C 6...|
|0016fa92cd6644feaa1383f09c2d2d54|[1C 18 5E 4E 61 6D 65 73 70 61 63 65 5F 64 65 66 61 75 6C 74 5F 74 61 62 6C 6...|
|0019d6000979ea66aa1383f09c2d2d54|[1C 18 5E 4E 61 6D 65 73 70 61 63 65 5F 64 65 66 61 75 6C 74 5F 74 61 62 6C 6...|
|0019df8931b4e3ceaa1383f09c2d2d54|[1C 18 58 4E 61 6D 65 73 70 61 63 65 5F 64 65 66 61 75 6C 74 5F 74 61 62 6C 6...|
|001d03a55c3b49dcf90c058f76ab6bf6|[1C 18 0A 41 70 70 65 6E 64 5F 6D 69 6E 1B 0D 88 07 43 6F 6E 74 65 78 74 0C 7...|
|001d92989ba792f5aa1383f09c2d2d54|[1C 18 5C 4E 61 6D 65 73 70 61 63 65 5F 64 65 66 61 75 6C 74 5F 74 61 62 6C 6...|
|001e04779e71221baa1383f09c2d2d54|[1C 18 5F 4E 61 6D 65 73 70 61 63 65 5F 64 65 66 61 75 6C 74 5F 74 61 62 6C 6...|
+--------------------------------+--------------------------------------------------------------------------------+

```


A DataFrame with two columns, `_1` and `_2` containing a String and a byte array.


# Processing time series in Spark


The data from your Warp 10 instance can now be loaded by Spark, time to process those data using WarpScript.


The transformation we will demonstrate simply reads the wrapper from the loaded data, unwraps it, extracts the class and labels of the GTS and the number of data points. Those two values are then grouped in a `Row` which will be returned by the function.


The WarpScript code to run is in a macro stored in `02.mc2`:


```
<%
'wrapper' STORE
$wrapper UNWRAP
DUP TOSELECTOR SWAP SIZE [ 'gts' 'size' ] STORE
// We return a structure (a Row instance), it is created
// via a list fed to ->SPARKROW
[ $gts $size ] ->SPARKROW
%>
```


In the Spark job we register a function which will be called with a reference to file `02.mc2` and with the data which should be passed as arguments to the macro. Spark is very strict on the signatures of the function that can be called in SparkSQL, so you may have to register many functions depending on the types they return and their umber of arguments.


The example `02.py` contains the following registration:


```
sqlContext.registerJavaFunction("ws", "io.warp10.spark.WarpScriptUDF2", "`gts` STRING, `count` LONG")
```


This registers function `ws` which will accept two parameters and will return a `Row` with columns `gts` and `count` of respective types `STRING` and `LONG`.


Spark allows up to 22 input arguments to the SparkSQL functions, so the Warp 10 Spark integration provides functions `io.warp10.spark.WarpScriptUDF1` to `io.warp10.spark.WarpScriptUDF22`!


Once you have adapted `02.py` to your Warp 10 instance, you can run the example using:


```
sh runspark.sh -f 02.m2 02.py
```


The option `-f 02.mc2` will instruct `spark-submit` to include the file `02.mc2` in the requirements of the job.


The result will be a DataFrame with two columns containing the `class{labels}` name of each GTS and the number of data points fetched for each one.


# Converting Geo Time Series to a DataFrame


The wrappers returned by the `Warp10InputFormat` are highly compressed and you should use them as often as possible in order to limit the memory footprint of your Spark job and the time taken to move data between stages.


That being said, it may be useful sometimes to convert Geo Time Series to a DataFrame containing *observations*, that is a value or a set of values, in the case of multi-variate time series, for a single timestamp.


Example `03.py` shows how to do this. The chunks of GTS are processed by the macro in `03.mc2` which emits a list of *gts*,*timestamp*,*value* tuples. That list is then [`explode`d](https://spark.apache.org/docs/latest/api/sql/index.html#explode) in the Spark job so each element of this list becomes a `Row` of the resulting DataFrame.


Launch this third example using:


```
sh runspark.sh -f 03.mc2 03.py
```


# Converting a DataFrame of observations to a Geo Time Series


Data may reside elsewhere than in Warp 10, for example in [Parquet](http://parquet.apache.org/), [ORC](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+ORC) or even simple CSV files. Yet it may be necessary to process the time series stored in those files using the Warp 10 Analytics Engine, it is therefore important to be able to convert observations from a Spark DataFrame into Geo Time Series.


Example `04.py` shows how this can be done. The data is in the CSV file `04.csv`. Each line contains a name of series, a timestamp and a value. The data will be loaded in a single DataFrame whose records are obvervations of different times series.


The next step is to group those records by series. Grouping of Spark DataFrame is possible but its sole purpose it to apply an aggregation function to the grouped data. The type of processing we want to do on the grouped data is not *per se* an aggregation function so we will use the grouping capabilities of the `RDD` API and then convert the grouped `RDD` back into a DataFrame for processing it via WarpScript.


In order to group an `RDD` it must contain pairs of elements so grouping can be done on the key. This is achieved by applying a lambda to the `RDD` underlying the DataFrame. Note that this will spawn an external Python interpreter and is therefore a costly operation. Unfortunately there is no alternative in PySpark.


After the call to `groupByKey` and the conversion back to a DataFrame, the records will contain the grouping key and a list containing an iterable on the grouped elements, a partition index and the length of the iterable. The iterable can be iterated over using [`FOREACH`](https://warp10.io/doc/FOREACH) in WarpScript. The benefit of this approach is that you can iterate on content which does not fit in memory, Spark will do the magic of reading the data from disk if the size requires so.


The processing of the iterable done in `04.mc2` will populate a GTS encoder and emit a wrapped version of it.


You can run this example via:


```
sh runspark.sh -f 04.mc2 04.py
```


# Writing data to Warp 10


The last piece of the puzzle is how to write data back to Warp 10. For this purpose, Warp 10 offers an [`OutputFormat`](https://hadoop.apache.org/docs/current/api/org/apache/hadoop/mapreduce/OutputFormat.html), namely the [`Warp10OutputFormat`](https://github.com/senx/warp10-platform/blob/master/warp10/src/main/java/io/warp10/hadoop/Warp10OutputFormat.java).


This `OutputFormat` will save an `RDD` containing key/value pairs whose value is a wrapped Geo Time Series or GTS Encoder (as produced via [`WRAPRAW`](https://warp10.io/doc/WRAPRAW)) into a Warp 10 instance.


Note that the `RDD` is produced by applying a lambda to the `RDD` underlying a DataFrame, this implies spawning an external Python interpreter as for preparing an `RDD` for grouping. Again this is mandatory in PySpark.


Example `05.py` shows how the `Warp10OutputFormat` is used. You will need to specify your own Warp 10 instance and the associated token. The example is then launched using:


```
sh runspark.sh 05.py
```


The data from `04.csv` will then be saved in your Warp 10 instance.


# Conclusion


In this blog post we have walked you through the use of the Warp 10 Analytics Engine in Spark. We hope that this gave you a good overview of the capabilities of this integration and that you will be able to use this approach for solving your time series problems at scale.


Do not hesitate to join [*The Warp 10 Lounge*](https://lounge.warp10.io/), the Slack community of Warp 10 users, where you will be able to discuss this topic and many more with like minded people.
