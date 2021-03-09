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
#PATH=${PATH}:${SPARK_HOME}/bin

HADOOP_HOME=${HADOOP_HOME-/opt/hadoop}
export YARN_CONF_DIR=${HADOOP_HOME}/etc/hadoop

export JAVA_HOME=${JAVA_HOME-/opt/java8}

command -v python >/dev/null 2>&1 || export PYSPARK_PYTHON=python2

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
