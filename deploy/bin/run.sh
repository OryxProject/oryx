#!/usr/bin/env bash

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

# The file compute-classpath.sh must be in the same directory as this file.

function usageAndExit {
  echo "Error: $1"
  echo "usage: run.sh [--option value] ..."
  echo "  --layer-jar  Oryx JAR file, like oryx-{serving,speed,batch}-x.y.z.jar"
  echo "  --conf       Oryx configuration file, like oryx.conf"
  echo "  --app-jar    Optional; default: none. User app JAR file]"
  echo "  --jvm-args   Optional; default: none. Extra args to Oryx JVM process"
  echo "  --deployment Optional; default: local. Only for Serving Layer now; can be 'yarn' or 'local'"
  exit 1
}

while (($#)); do
  if [ "$1" == "--layer-jar" ]; then
    LAYER_JAR=$2
  elif [ "$1" == "--conf" ]; then
    CONFIG_FILE=$2
  elif [ "$1" == "--app-jar" ]; then
    APP_JAR=$2
  elif [ "$1" == "--jvm-args" ]; then
    JVM_ARGS=$2
  elif [ "$1" == "--deployment" ]; then
    DEPLOYMENT=$2
  fi
  shift
done

if [ -z "${LAYER_JAR}" ]; then
  usageAndExit "No layer JAR specified"
fi
if [ -z "${CONFIG_FILE}" ]; then
  usageAndExit "No config file specified"
fi
if [ ! -f "${LAYER_JAR}" ]; then
  usageAndExit "Layer JAR does not exist"
fi
if [ ! -f "${CONFIG_FILE}" ]; then
  usageAndExit "Config file does not exist"
fi

COMPUTE_CLASSPATH="compute-classpath.sh"
if [ ! -x "$COMPUTE_CLASSPATH" ]; then
  usageAndExit "$COMPUTE_CLASSPATH does not exist or isn't executable"
fi

LAYER=`echo ${LAYER_JAR} | grep -oE "oryx-[^-]+"`
LAYER_JAR_NAME=`basename ${LAYER_JAR}`
CONFIG_FILE_NAME=`basename ${CONFIG_FILE}`
if [ -n "${APP_JAR}" ]; then
  APP_JAR_NAME=`basename ${APP_JAR}`
fi
BASE_CLASSPATH=`./${COMPUTE_CLASSPATH} | paste -s -d: -`
# Need to ship examples JAR with app as it conveniently contains right Kafka, in Spark
SPARK_EXAMPLES_JAR=`./${COMPUTE_CLASSPATH} | grep spark-examples`
YARN_DIST_SHELL_JAR=`./${COMPUTE_CLASSPATH} | grep distributedshell`

SPARK_STREAMING_JARS="${LAYER_JAR},${SPARK_EXAMPLES_JAR}"
if [ "${APP_JAR}" != "" ]; then
  SPARK_STREAMING_JARS="${APP_JAR},${SPARK_STREAMING_JARS}"
fi
SPARK_STREAMING_PROPS="-Dspark.yarn.dist.files=${CONFIG_FILE} \
 -Dspark.jars=${SPARK_STREAMING_JARS} \
 -Dsun.io.serialization.extendeddebuginfo=true \
 -Dspark.executor.extraJavaOptions=\"-Dconfig.file=${CONFIG_FILE_NAME} -Dsun.io.serialization.extendeddebuginfo=true\""

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
    # Only for Serving Layer now
    if [ "${DEPLOYMENT}" == "yarn" ]; then
      CONFIG_LINE=`grep -oE "^[^#]*" ${CONFIG_FILE} | tr -d '\n'`
      YARN_MEMORY_MB=`echo ${CONFIG_LINE} | grep -oE "serving\\s*=?\\s*{.+yarn\\s*=?\\s*{.+memory\\s*=\\s*\\S+" | grep -oE "\\S+$" | grep -oE "[0-9]+"`
      YARN_CORES=`echo ${CONFIG_LINE} | grep -oE "serving\\s*=?\\s*{.+yarn\\s*=?\\s*{.+cores\\s*=\\s*\\S+" | grep -oE "\\S+$" | grep -oE "[0-9]+"`
      YARN_INSTANCES=`echo ${CONFIG_LINE} | grep -oE "serving\\s*=?\\s*{.+yarn\\s*=?\\s*{.+instances\\s*=\\s*\\S+" | grep -oE "\\S+$" | grep -oE "[0-9]+"`
      # Unfortunately we duplicate defaults here:
      if [ -z "${YARN_MEMORY_MB}" ]; then
        YARN_MEMORY_MB="4000"
      fi
      if [ -z "${YARN_CORES}" ]; then
        YARN_CORES="4"
      fi
      if [ -z "${YARN_INSTANCES}" ]; then
        YARN_INSTANCES="1"
      fi
      YARN_APP_NAME="OryxServingLayer"
      JVM_HEAP_MB=`echo "${YARN_MEMORY_MB} * 0.9" | bc | grep -oE "^[0-9]+"`
      EXTRA_PROPS="-Xmx${JVM_HEAP_MB}m"
    fi
    ;;
  *)
    usageAndExit "Bad layer ${LAYER}"
    ;;
esac

if [ -n "${YARN_APP_NAME}" ]; then

  FINAL_CLASSPATH="${LAYER_JAR_NAME}:${BASE_CLASSPATH}"
  if [ -n "${APP_JAR_NAME}" ]; then
    FINAL_CLASSPATH="${APP_JAR_NAME}:${FINAL_CLASSPATH}"
  fi

  PRN="$$"
  LOCAL_SCRIPT_DIR="/tmp/OryxServingLayer/${PRN}"
  LOCAL_SCRIPT="${LOCAL_SCRIPT_DIR}/run-yarn.sh"
  YARN_LOG4J="${LOCAL_SCRIPT_DIR}/log4j.properties"

  HDFS_APP_DIR="/tmp/OryxServingLayer/${PRN}"

  mkdir -p ${LOCAL_SCRIPT_DIR}
  hdfs dfs -mkdir -p ${HDFS_APP_DIR}
  hdfs dfs -copyFromLocal -f ${LAYER_JAR} ${CONFIG_FILE} ${HDFS_APP_DIR}/
  if [ -n "${APP_JAR}" ]; then
    hdfs dfs -copyFromLocal -f ${APP_JAR} ${HDFS_APP_DIR}/
  fi

  echo "log4j.logger.org.apache.hadoop.yarn.applications.distributedshell=WARN" >> ${YARN_LOG4J}
  echo "hdfs dfs -copyToLocal ${HDFS_APP_DIR}/* ." >> ${LOCAL_SCRIPT}
  echo "java ${JVM_ARGS} ${EXTRA_PROPS} -Dconfig.file=${CONFIG_FILE_NAME} -cp ${FINAL_CLASSPATH} ${MAIN_CLASS}" >> ${LOCAL_SCRIPT}

  yarn jar ${YARN_DIST_SHELL_JAR} \
    -jar ${YARN_DIST_SHELL_JAR} \
    org.apache.hadoop.yarn.applications.distributedshell.Client \
    -appname ${YARN_APP_NAME} \
    -container_memory ${YARN_MEMORY_MB} \
    -container_vcores ${YARN_CORES} \
    -num_containers ${YARN_INSTANCES} \
    -log_properties ${YARN_LOG4J} \
    -shell_script ${LOCAL_SCRIPT}

else

  FINAL_CLASSPATH="${LAYER_JAR}:${BASE_CLASSPATH}"
  if [ -n "${APP_JAR}" ]; then
    FINAL_CLASSPATH="${APP_JAR}:${FINAL_CLASSPATH}"
  fi
  java ${JVM_ARGS} ${EXTRA_PROPS} -Dconfig.file=${CONFIG_FILE} -cp ${FINAL_CLASSPATH} ${MAIN_CLASS}

fi
