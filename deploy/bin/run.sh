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

# Usage: run.sh --layer-jar [Oryx .jar file] --conf [Oryx .conf file]
#               --app-jar [user .jar file] --jvm-args [args]

function usageAndExit {
  echo "Error: $1"
  echo "usage: run.sh --layer-jar oryx-{serving,speed,batch}-x.y.z.jar --conf my.conf \
        [--app-jar my-app.jar] [--jvm-args 'jvm args']"
  exit 1
}

while (($#)); do
  if [ "$1" = "--layer-jar" ]; then
    LAYER_JAR=$2
  elif [ "$1" = "--conf" ]; then
    CONFIG_FILE=$2
  elif [ "$1" = "--app-jar" ]; then
    APP_JAR=$2
  elif [ "$1" = "--jvm-args" ]; then
    JVM_ARGS=$2
  fi
  shift
done

if [ "${LAYER_JAR}" == "" ]; then
  usageAndExit "No layer JAR specified"
fi
if [ "${CONFIG_FILE}" == "" ]; then
  usageAndExit "No config file specified"
fi
if [ ! -f "${LAYER_JAR}" ]; then
  usageAndExit "Layer JAR does not exist"
fi
if [ ! -f "${CONFIG_FILE}" ]; then
  usageAndExit "Config file does not exist"
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
 -Dsun.io.serialization.extendeddebuginfo=true \
 -Dspark.executor.extraJavaOptions=-Dconfig.file=${CONFIG_FILE_NAME}"
# Note: extraJavaOptions has a problem with spaces in its value before Spark 1.3
# TODO: later, re-add -Dsun.io.serialization.extendeddebuginfo=true here too

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
    usageAndExit "Bad layer ${LAYER}"
    ;;
esac

FINAL_CLASSPATH="${LAYER_JAR}:${BASE_CLASSPATH}"
if [ "${APP_JAR}" != "" ]; then
  FINAL_CLASSPATH="${APP_JAR}:${FINAL_CLASSPATH}"
fi

java ${JVM_ARGS} ${EXTRA_PROPS} -Dconfig.file=${CONFIG_FILE} -cp ${FINAL_CLASSPATH} ${MAIN_CLASS}
