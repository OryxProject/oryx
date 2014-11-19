#!/bin/bash

# Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
#
# Cloudera, Inc. licenses this file to you under the Apache License,
# Version 2.0 (the "License"). You may not use this file except in
# compliance with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for
# the specific language governing permissions and limitations under the
# License.

# Usage: run.sh --layer-jar [Oryx .jar file] --conf [Oryx .conf file] --app-jar [user .jar file]

while (($#)); do
  if [ "$1" = "--layer-jar" ]; then
    LAYER_JAR=$2
  elif [ "$1" = "--conf" ]; then
    CONFIG_FILE=$2
  elif [ "$1" = "--app-jar" ]; then
    APP_JAR=$2
  fi
  shift
done

if [ "${LAYER_JAR}" == "" ]; then
  echo "No layer JAR specified"
  exit 1
fi
if [ "${CONFIG_FILE}" == "" ]; then
  echo "No config file specified"
  exit 1
fi
if [ ! -f "${LAYER_JAR}" ]; then
  echo "Layer JAR does not exist"
  exit 1
fi
if [ ! -f "${CONFIG_FILE}" ]; then
  echo "Config file does not exist"
  exit 1
fi

LAYER=`echo ${LAYER_JAR} | grep -oE "oryx-[^-]+"`
CONFIG_FILE_NAME=`basename ${CONFIG_FILE}`
BASE_CLASSPATH=`./compute-classpath.sh | paste -s -d: -`
# Need to ship examples JAR with app as it conveniently contains right Kafka, in Spark
SPARK_EXAMPLES_JAR=`./compute-classpath.sh | grep spark-examples`

SPARK_STREAMING_JARS="${LAYER_JAR},${SPARK_EXAMPLES_JAR}"
if [ "${APP_JAR}" != "" ]; then
  SPARK_STREAMING_JARS="${APP_JAR},${SPARK_STREAMING_JARS}"
fi
SPARK_STREAMING_PROPS="-Dspark.yarn.dist.files=${CONFIG_FILE} \
 -Dspark.jars=${SPARK_STREAMING_JARS} \
 -Dspark.executor.extraJavaOptions=\"-Dconfig.file=${CONFIG_FILE_NAME}\""

case "${LAYER}" in
  oryx-batch)
    MAIN_CLASS="com.cloudera.oryx.batch.Main"
    EXTRA_PROPS="${SPARK_STREAMING_PROPS}"
    ;;
  oryx-speed)
    MAIN_CLASS="com.cloudera.oryx.speed.Main"
    EXTRA_PROPS="${SPARK_STREAMING_PROPS}"
    ;;
  oryx-serving)
    MAIN_CLASS="com.cloudera.oryx.serving.Main"
    ;;
  *)
    echo "Bad layer ${LAYER}"
    exit 1
    ;;
esac

FINAL_CLASSPATH="${LAYER_JAR}:${BASE_CLASSPATH}"
if [ "${APP_JAR}" != "" ]; then
  FINAL_CLASSPATH="${APP_JAR}:${FINAL_CLASSPATH}"
fi

java -cp ${FINAL_CLASSPATH} -Dconfig.file=${CONFIG_FILE} ${EXTRA_PROPS} ${MAIN_CLASS}
