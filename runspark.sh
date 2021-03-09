#!/bin/sh
#
# Copyright 2021,  SenX S.A.S.
#

##
## Extract files
##

FILES=""
while getopts f: o
do
  FILES="${FILES}${OPTARG},"
done
shift `expr ${OPTIND} - 1`

DRIVER_MEMORY=2G
EXECUTOR_MEMORY=2G

export SPARK_HOME=/opt/spark-2.4.5-bin-hadoop2.7

if [ ! -f ${SPARK_HOME}/bin/spark-submit ]
then
  echo "### Cannot find spark-submit in '${SPARK_HOME}/bin', ensure SPARK_HOME points to your Spark install"
  exit 1
fi

export JAVA_HOME=${JAVA_HOME-/opt/java8}

if [ ! -f ${JAVA_HOME}/bin/java ]
then
  echo "### Cannot find java in '${JAVA_HOME}/bin', ensure JAVA_HOME is correctly set"
  exit 1
fi

export PYSPARK_PYTHON=python
command -v ${PYSPARK_PYTHON} >/dev/null 2>&1 || export PYSPARK_PYTHON=python2

##
## Use the config below to run the Spark job locally
##

MASTER=local[1]

##
## Use the config below to run the Spark job on YARN
##

#MASTER=yarn

##
## Number of executors to request and number of parallel executions per executor
##

NEXECS=1
NCORES=2

##
## Execute Spark submit
##

FILES="${FILES}warp10.conf"

exec ${SPARK_HOME}/bin/spark-submit --master "${MASTER}" \
       --deploy-mode client \
       --executor-cores ${NCORES} \
       --num-executors ${NEXECS} \
       --driver-memory ${DRIVER_MEMORY} \
       --executor-memory ${EXECUTOR_MEMORY} \
       --properties-file spark.conf \
       --packages io.warp10:warp10-spark:1.0.5 \
       --repositories https://dl.bintray.com/senx/maven \
       --files ${FILES} $@
