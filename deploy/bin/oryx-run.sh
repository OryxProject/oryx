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

# TODO: remove the '2>&1 | grep -vE "^mkdir: cannot create directory"' used in several
# Kafka commands below. This suppresses an ignorable warning from the CDH 5.4 distro.

function usageAndExit {
  echo "Error: $1"
  echo "usage: oryx-run.sh command [--option value] ..."
  echo "  where command is one of:"
  echo "    batch        Run Batch Layer"
  echo "    speed        Run Speed Layer"
  echo "    serving      Run Serving Layer"
  echo "    kafka-setup  Inspect ZK/Kafka config and configure Kafka topics"
  echo "    kafka-tail   Follow output from Kafka topics"
  echo "    kafka-input  Push data to input topic"
  echo "  and options are one of:"
  echo "    --layer-jar  Oryx JAR file, like oryx-{serving,speed,batch}-x.y.z.jar"
  echo "                 Defaults to any oryx-*.jar in working dir"
  echo "    --conf       Oryx configuration file, like oryx.conf. Defaults to 'oryx.conf'"
  echo "    --app-jar    User app JAR file"
  echo "    --jvm-args   Extra args to Oryx JVM process"
  echo "    --deployment Only for Serving Layer now; can be 'yarn' or 'local', Default: local."
  echo "    --input-file Only for kafka-input. Input file to send"
  exit 1
}

COMMAND=$1
shift

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
  elif [ "$1" == "--input-file" ]; then
    INPUT_FILE=$2
  fi
  shift
done

if [ -z "${LAYER_JAR}" ]; then
  case "${COMMAND}" in
  batch|speed|serving)
    LAYER_JAR=`ls -1 oryx-${COMMAND}-*.jar`
    ;;
  *)
    LAYER_JAR=`ls -1 oryx-*.jar | head -1`
    ;;
  esac
fi
if [ -z "${CONFIG_FILE}" ]; then
  CONFIG_FILE="oryx.conf"
fi
if [ ! -f "${LAYER_JAR}" ]; then
  usageAndExit "Layer JAR ${LAYER_JAR} does not exist"
fi
if [ ! -f "${CONFIG_FILE}" ]; then
  usageAndExit "Config file ${CONFIG_FILE} does not exist"
fi

CONFIG_PROPS=`java -cp ${LAYER_JAR} -Dconfig.file=${CONFIG_FILE} com.cloudera.oryx.common.settings.ConfigToProperties`

case "${COMMAND}" in
batch|speed|serving)

  # Main Layer handling script

  CONFIG_FILE_NAME=`basename ${CONFIG_FILE}`
  if [ -n "${APP_JAR}" ]; then
    APP_JAR_NAME=`basename ${APP_JAR}`
  fi

  COMPUTE_CLASSPATH="compute-classpath.sh"
  if [ ! -x "$COMPUTE_CLASSPATH" ]; then
    usageAndExit "$COMPUTE_CLASSPATH script does not exist or isn't executable"
  fi
  BASE_CLASSPATH=`bash ${COMPUTE_CLASSPATH} | paste -s -d: -`
  # Need to ship examples JAR with app as it conveniently contains right Kafka, in Spark
  SPARK_EXAMPLES_JAR=`bash ${COMPUTE_CLASSPATH} | grep spark-examples`

  SPARK_STREAMING_JARS="${LAYER_JAR},${SPARK_EXAMPLES_JAR}"
  if [ "${APP_JAR}" != "" ]; then
    SPARK_STREAMING_JARS="${APP_JAR},${SPARK_STREAMING_JARS}"
  fi
  SPARK_STREAMING_PROPS="-Dspark.yarn.dist.files=${CONFIG_FILE} \
   -Dspark.jars=${SPARK_STREAMING_JARS} \
   -Dsun.io.serialization.extendeddebuginfo=true \
   -Dspark.executor.extraJavaOptions=\"-Dconfig.file=${CONFIG_FILE_NAME} -Dsun.io.serialization.extendeddebuginfo=true\""

  MAIN_CLASS="com.cloudera.oryx.${COMMAND}.Main"
  case "${COMMAND}" in
    batch)
      EXTRA_PROPS="${SPARK_STREAMING_PROPS}"
      ;;
    speed)
      EXTRA_PROPS="${SPARK_STREAMING_PROPS}"
      ;;
    serving)
      MEMORY_MB=`echo "${CONFIG_PROPS}" | grep -E "^oryx\.serving\.memory=.+$" | grep -oE "[^=]+$" | grep -oE "[0-9]+"`
      # Only for Serving Layer now
      if [ "${DEPLOYMENT}" == "yarn" ]; then
        YARN_CORES=`echo "${CONFIG_PROPS}" | grep -E "^oryx\.serving\.yarn\.cores=.+$" | grep -oE "[^=]+$"`
        YARN_INSTANCES=`echo "${CONFIG_PROPS}" | grep -E "^oryx\.serving\.yarn\.instances=.+$" | grep -oE "[^=]+$"`
        APP_ID=`echo "${CONFIG_PROPS}" | grep -E "^oryx\.id=.+$" | grep -oE "[^=]+$"`
        YARN_APP_NAME="OryxServingLayer${APP_ID}"
        JVM_HEAP_MB=`echo "${MEMORY_MB} * 0.9" | bc | grep -oE "^[0-9]+"`
      else
        JVM_HEAP_MB=${MEMORY_MB}
      fi
      EXTRA_PROPS="-Xmx${JVM_HEAP_MB}m"
      ;;
  esac

  if [ -n "${YARN_APP_NAME}" ]; then
    # Launch layer in YARN

    LAYER_JAR_NAME=`basename ${LAYER_JAR}`
    FINAL_CLASSPATH="${LAYER_JAR_NAME}:${BASE_CLASSPATH}"
    if [ -n "${APP_JAR_NAME}" ]; then
      FINAL_CLASSPATH="${APP_JAR_NAME}:${FINAL_CLASSPATH}"
    fi

    LOCAL_SCRIPT_DIR="/tmp/${YARN_APP_NAME}"
    LOCAL_SCRIPT="${LOCAL_SCRIPT_DIR}/run-yarn.sh"
    YARN_LOG4J="${LOCAL_SCRIPT_DIR}/log4j.properties"
    # YARN_APP_NAME will match the base of what distributedshell uses, in the home dir
    HDFS_APP_DIR="${YARN_APP_NAME}"

    # Only one copy of the app can be running anyway, so fail if it already seems
    # to be running due to presence of directories
    if [ -d ${LOCAL_SCRIPT_DIR} ]; then
      usageAndExit "${LOCAL_SCRIPT_DIR} already exists; is ${YARN_APP_NAME} running?"
    fi
    if hdfs dfs -test -d ${HDFS_APP_DIR}; then
      usageAndExit "${HDFS_APP_DIR} already exists; is ${YARN_APP_NAME} running?"
    fi

    # Make temp directories to stage resources, locally and in HDFS
    mkdir -p ${LOCAL_SCRIPT_DIR}
    hdfs dfs -mkdir -p ${HDFS_APP_DIR}
    hdfs dfs -put ${LAYER_JAR} ${CONFIG_FILE} ${HDFS_APP_DIR}/
    if [ -n "${APP_JAR}" ]; then
      hdfs dfs -put ${APP_JAR} ${HDFS_APP_DIR}/
    fi

    echo "log4j.logger.org.apache.hadoop.yarn.applications.distributedshell=WARN" >> ${YARN_LOG4J}

    # Need absolute path
    OWNER=`hdfs dfs -stat '%u' ${HDFS_APP_DIR}`
    echo "hdfs dfs -get /user/${OWNER}/${HDFS_APP_DIR}/* ." >> ${LOCAL_SCRIPT}
    echo "java ${EXTRA_PROPS} -Dconfig.file=${CONFIG_FILE_NAME} ${JVM_ARGS} -cp ${FINAL_CLASSPATH} ${MAIN_CLASS}" >> ${LOCAL_SCRIPT}

    YARN_DIST_SHELL_JAR=`bash ${COMPUTE_CLASSPATH} | grep distributedshell`

    echo "Running ${YARN_INSTANCES} ${YARN_APP_NAME} (${YARN_CORES} cores / ${YARN_MEMORY_MB}MB)"
    echo "Note that you will need to find the Application Master in YARN to find the Serving Layer"
    echo "instances, and kill the application with 'yarn application -kill [app ID]'"
    echo

    yarn jar ${YARN_DIST_SHELL_JAR} \
      -jar ${YARN_DIST_SHELL_JAR} \
      org.apache.hadoop.yarn.applications.distributedshell.Client \
      -appname ${YARN_APP_NAME} \
      -container_memory ${YARN_MEMORY_MB} \
      -container_vcores ${YARN_CORES} \
      -master_memory 256 \
      -master_vcores 1 \
      -num_containers ${YARN_INSTANCES} \
      -log_properties ${YARN_LOG4J} \
      -timeout 2147483647 \
      -shell_script ${LOCAL_SCRIPT}

    # TODO timeout above is the max, is 24 days, and can't be disabled

    # Clean up temp dirs; they are only used by this application anyway
    hdfs dfs -rm -r -skipTrash "${HDFS_APP_DIR}"
    rm -r "${LOCAL_SCRIPT_DIR}"

  else
    # Launch Layer as local process

    FINAL_CLASSPATH="${LAYER_JAR}:${BASE_CLASSPATH}"
    if [ -n "${APP_JAR}" ]; then
      FINAL_CLASSPATH="${APP_JAR}:${FINAL_CLASSPATH}"
    fi
    java ${EXTRA_PROPS} -Dconfig.file=${CONFIG_FILE} ${JVM_ARGS} -cp ${FINAL_CLASSPATH} ${MAIN_CLASS}

  fi
  ;;

kafka-setup|kafka-tail|kafka-input)

  INPUT_ZK=`echo "${CONFIG_PROPS}" | grep -E "^oryx\.input-topic\.lock\.master=.+$" | grep -oE "[^=]+$"`
  INPUT_KAFKA=`echo "${CONFIG_PROPS}" | grep -E "^oryx\.input-topic\.broker=.+$" | grep -oE "[^=]+$"`
  INPUT_TOPIC=`echo "${CONFIG_PROPS}" | grep -E "^oryx\.input-topic\.message\.topic=.+$" | grep -oE "[^=]+$"`
  UPDATE_ZK=`echo "${CONFIG_PROPS}" | grep -E "^oryx\.update-topic\.lock\.master=.+$" | grep -oE "[^=]+$"`
  UPDATE_KAFKA=`echo "${CONFIG_PROPS}" | grep -E "^oryx\.update-topic\.broker=.+$" | grep -oE "[^=]+$"`
  UPDATE_TOPIC=`echo "${CONFIG_PROPS}" | grep -E "^oryx\.update-topic\.message\.topic=.+$" | grep -oE "[^=]+$"`

  echo "Input   ZK      ${INPUT_ZK}"
  echo "        Kafka   ${INPUT_KAFKA}"
  echo "        topic   ${INPUT_TOPIC}"
  echo "Update  ZK      ${INPUT_ZK}"
  echo "        Kafka   ${UPDATE_KAFKA}"
  echo "        topic   ${UPDATE_TOPIC}"
  echo

  case "${COMMAND}" in
  kafka-setup)
    ALL_TOPICS=`kafka-topics --list --zookeeper ${INPUT_ZK} 2>&1 | grep -vE "^mkdir: cannot create directory"`
    echo "All available topics:"
    echo "${ALL_TOPICS}"
    echo

    if [ -z `echo "${ALL_TOPICS}" | grep ${INPUT_TOPIC}` ]; then
      read -p "Input topic ${INPUT_TOPIC} does not exist. Create it? " CREATE
      case "${CREATE}" in
        y|Y)
          echo "Creating topic ${INPUT_TOPIC}"
          kafka-topics --zookeeper ${INPUT_ZK} --create --replication-factor 2 --partitions 1 --topic ${INPUT_TOPIC} 2>&1 | grep -vE "^mkdir: cannot create directory"
          ;;
      esac
    fi
    echo "Status of topic ${INPUT_TOPIC}:"
    kafka-topics --zookeeper ${INPUT_ZK} --describe --topic ${INPUT_TOPIC} 2>&1 | grep -vE "^mkdir: cannot create directory"
    echo

    if [ -z `echo "${ALL_TOPICS}" | grep ${UPDATE_TOPIC}` ]; then
      read -p "Update topic ${UPDATE_TOPIC} does not exist. Create it? " CREATE
      case "${CREATE}" in
        y|Y)
          echo "Creating topic ${UPDATE_TOPIC}"
          kafka-topics --zookeeper ${UPDATE_ZK} --create --replication-factor 2 --partitions 1 --topic ${UPDATE_TOPIC} 2>&1 | grep -vE "^mkdir: cannot create directory"
          kafka-topics --zookeeper ${UPDATE_ZK} --alter --topic ${UPDATE_TOPIC} --config retention.ms=86400000 2>&1 | grep -vE "^mkdir: cannot create directory"
          ;;
      esac
    fi
    echo "Status of topic ${UPDATE_TOPIC}:"
    kafka-topics --zookeeper ${UPDATE_ZK} --describe --topic ${UPDATE_TOPIC} 2>&1 | grep -vE "^mkdir: cannot create directory"
    echo
    ;;

  kafka-tail)
    kafka-console-consumer --zookeeper ${INPUT_ZK} --whitelist ${INPUT_TOPIC},${UPDATE_TOPIC} 2>&1 | grep -vE "^mkdir: cannot create directory"
    ;;

  kafka-input)
    if [ ! -f "${INPUT_FILE}" ]; then
      usageAndExit "Input file ${INPUT_FILE} does not exist"
    fi
    kafka-console-producer --broker-list ${INPUT_KAFKA} --topic ${INPUT_TOPIC} < "${INPUT_FILE}" 2>&1 | grep -vE "^mkdir: cannot create directory"
    ;;

  esac
  ;;

*)
  usageAndExit "Invalid command ${COMMAND}"
  ;;

esac
