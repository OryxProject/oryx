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

# Usage: run.sh [Oryx .jar file] [Oryx .conf file]

APP_JAR=$1
CONFIG_FILE=$2

APP=`echo ${APP_JAR} | grep -oE "oryx-[^-]+"`
CONFIG_FILE_NAME=`basename ${CONFIG_FILE}`
ORYX_CLASSPATH=`./compute-classpath.sh | paste -s -d: -`
# Need to ship examples JAR with app as it conveniently contains right Kafka, in Spark
SPARK_EXAMPLES_JAR=`./compute-classpath.sh | grep spark-examples`

case "${APP}" in
  oryx-batch)
    MAIN_CLASS="com.cloudera.oryx.batch.Main"
    EXTRA_PROPS="-Dspark.yarn.dist.files=${CONFIG_FILE} \
      -Dspark.jars=${APP_JAR},${SPARK_EXAMPLES_JAR} \
      -Dspark.executor.extraJavaOptions=-Dconfig.file=${CONFIG_FILE_NAME}"
    ;;
  oryx-speed)
    MAIN_CLASS="com.cloudera.oryx.speed.Main"
    EXTRA_PROPS="-Dspark.yarn.dist.files=${CONFIG_FILE} \
      -Dspark.jars=${APP_JAR},${SPARK_EXAMPLES_JAR} \
      -Dspark.executor.extraJavaOptions=-Dconfig.file=${CONFIG_FILE_NAME}"
    ;;
  oryx-serving)
    MAIN_CLASS="com.cloudera.oryx.serving.Main"
    EXTRA_PROPS=""
    ;;
  *)
    echo "Bad app ${APP}"
    exit 1
    ;;
esac

java -cp ${APP_JAR}:${ORYX_CLASSPATH} -Dconfig.file=${CONFIG_FILE} ${EXTRA_PROPS} ${MAIN_CLASS}
