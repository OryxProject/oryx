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
  echo "$1"
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
  echo "    --jvm-args   Extra args to Oryx JVM processes (including drivers and executors)"
  echo "    --deployment Only for Serving Layer now; can be 'yarn' or 'local', Default: local."
  echo "    --input-file Only for kafka-input. Input file to send"
  echo "    --help       Display this message"
  exit 1
}

if [ "$1" == "--help" ]; then
  usageAndExit
fi

COMMAND=$1
shift

while (($#)); do
  if [ "$1" == "--layer-jar" ]; then
    LAYER_JAR="$2"
  elif [ "$1" == "--conf" ]; then
    CONFIG_FILE="$2"
  elif [ "$1" == "--app-jar" ]; then
    APP_JAR="$2"
  elif [ "$1" == "--jvm-args" ]; then
    JVM_ARGS="$2"
  elif [ "$1" == "--deployment" ]; then
    DEPLOYMENT="$2"
  elif [ "$1" == "--input-file" ]; then
    INPUT_FILE="$2"
  else
    usageAndExit "Unrecognized option $1"
  fi
  shift
  shift
done

if [ -z "${LAYER_JAR}" ]; then
  case "${COMMAND}" in
  batch|speed|serving)
    LAYER_JAR=`ls -1 oryx-${COMMAND}-*.jar 2> /dev/null`
    ;;
  *)
    LAYER_JAR=`ls -1 oryx-batch-*.jar oryx-speed-*.jar oryx-serving-*.jar 2> /dev/null | head -1`
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
if [ -z "${CONFIG_PROPS}" ]; then
  usageAndExit "Config file ${CONFIG_FILE} could not be parsed"
fi

# If first arg is FOO and second is bar, and CONFIG_PROPS contains a property bar=baz, then
# environment variable FOO is set to value baz by this function. The second argument must be
# expressed as a regular expression; "foo\.bar" not "foo.bar"
function setVarFromProperty {
  local __resultvar=$1
  local property=$2
  local result=`echo "${CONFIG_PROPS}" | grep -E "^${property}=.+$" | grep -oE "[^=]+$"`
  eval $__resultvar=$result
}

case "${COMMAND}" in
kafka-setup|kafka-tail|kafka-input)
  # Helps execute kafka-foo or kafka-foo.sh as appropriate.
  # Kind of assume we're using all one or the other
  if [ -x "$(command -v kafka-topics)" ]; then
    KAFKA_TOPICS_SH="kafka-topics"
    KAFKA_CONSOLE_CONSUMER_SH="kafka-console-consumer"
    KAFKA_CONSOLE_PRODUCER_SH="kafka-console-producer"
  elif [ -x "$(command -v kafka-topics.sh)" ]; then
    KAFKA_TOPICS_SH="kafka-topics.sh"
    KAFKA_CONSOLE_CONSUMER_SH="kafka-console-consumer.sh"
    KAFKA_CONSOLE_PRODUCER_SH="kafka-console-producer.sh"
  else
    echo "Can't find kafka scripts like kafka-topics"
    exit 2
  fi
  ;;
esac

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

  MAIN_CLASS="com.cloudera.oryx.${COMMAND}.Main"

  setVarFromProperty "APP_ID" "oryx\.id"

  case "${COMMAND}" in
  batch|speed)
    # Need to ship examples JAR with app as it conveniently contains right Kafka, in Spark
    SPARK_EXAMPLES_JAR=`bash ${COMPUTE_CLASSPATH} | grep spark-examples`

    SPARK_STREAMING_JARS="${SPARK_EXAMPLES_JAR}"
    if [ -n "${APP_JAR}" ]; then
      SPARK_STREAMING_JARS="${APP_JAR},${SPARK_STREAMING_JARS}"
    fi
    SPARK_DRIVER_JAVA_OPTS="-Dconfig.file=${CONFIG_FILE}"
    SPARK_EXECUTOR_JAVA_OPTS="-Dconfig.file=${CONFIG_FILE_NAME}"
    if [ -n "${JVM_ARGS}" ]; then
      if [[ "${JVM_ARGS}" == *"-Xmx"* ]]; then
        echo "Warning: -Xmx is set in --jvm-args, but it will be overridden by .conf file settings";
      fi
      SPARK_DRIVER_JAVA_OPTS="${JVM_ARGS} ${SPARK_DRIVER_JAVA_OPTS}"
      SPARK_EXECUTOR_JAVA_OPTS="${JVM_ARGS} ${SPARK_EXECUTOR_JAVA_OPTS}"
    fi

    # Force to spark-submit for Spark-based batch/speed layer
    DEPLOYMENT="spark-submit"
    case "${COMMAND}" in
    batch)
      APP_NAME="OryxBatchLayer-${APP_ID}"
      ;;
    speed)
      APP_NAME="OryxSpeedLayer-${APP_ID}"
      ;;
    esac

    setVarFromProperty "SPARK_MASTER" "oryx\.${COMMAND}\.streaming\.master"
    setVarFromProperty "DRIVER_MEMORY" "oryx\.${COMMAND}\.streaming\.driver-memory"
    setVarFromProperty "EXECUTOR_MEMORY" "oryx\.${COMMAND}\.streaming\.executor-memory"
    setVarFromProperty "EXECUTOR_CORES" "oryx\.${COMMAND}\.streaming\.executor-cores"
    setVarFromProperty "NUM_EXECUTORS" "oryx\.${COMMAND}\.streaming\.num-executors"
    setVarFromProperty "DYNAMIC_ALLOCATION" "oryx\.${COMMAND}\.streaming\.dynamic-allocation"
    setVarFromProperty "SPARK_UI_PORT" "oryx\.${COMMAND}\.ui\.port"
    SPARK_EXTRA_CONFIG=`echo "${CONFIG_PROPS}" | grep -E "^oryx\.${COMMAND}\.streaming\.config\..+=.+$" | grep -oE "spark.+$"`
    ;;

  serving)
    setVarFromProperty "MEMORY_MB" "oryx\.serving\.memory"
    MEMORY_MB=`echo ${MEMORY_MB} | grep -oE "[0-9]+"`
    # Only for Serving Layer now
    case "${DEPLOYMENT}" in
    yarn)
      setVarFromProperty "YARN_CORES" "oryx\.serving\.yarn\.cores"
      setVarFromProperty "YARN_INSTANCES" "oryx\.serving\.yarn\.instances"
      APP_NAME="OryxServingLayer-${APP_ID}"
      JVM_HEAP_MB=`echo "${MEMORY_MB} * 0.9" | bc | grep -oE "^[0-9]+"`
      ;;
    *)
      JVM_HEAP_MB=${MEMORY_MB}
      ;;
    esac
    EXTRA_PROPS="-Xmx${JVM_HEAP_MB}m"
    ;;
  esac

  case "${DEPLOYMENT}" in
  spark-submit)
    # Launch Spark-based layer with spark-submit

    if [ -x "$(command -v spark2-submit)" ]; then
      SPARK_SUBMIT_SCRIPT="spark2-submit"
    else
      SPARK_SUBMIT_SCRIPT="spark-submit"
    fi

    SPARK_SUBMIT_CMD="${SPARK_SUBMIT_SCRIPT} --master ${SPARK_MASTER} --name ${APP_NAME} --class ${MAIN_CLASS} \
     --jars ${SPARK_STREAMING_JARS} --files ${CONFIG_FILE} --driver-memory ${DRIVER_MEMORY} \
     --driver-java-options \"${SPARK_DRIVER_JAVA_OPTS}\" --executor-memory ${EXECUTOR_MEMORY} --executor-cores ${EXECUTOR_CORES} \
     --conf spark.executor.extraJavaOptions=\"${SPARK_EXECUTOR_JAVA_OPTS}\" --conf spark.ui.port=${SPARK_UI_PORT}"
    for SPARK_KEY_VALUE_CONF in ${SPARK_EXTRA_CONFIG}; do
      SPARK_SUBMIT_CMD="${SPARK_SUBMIT_CMD} --conf ${SPARK_KEY_VALUE_CONF}"
    done
    case "${DYNAMIC_ALLOCATION}" in
    true)
      SPARK_SUBMIT_CMD="${SPARK_SUBMIT_CMD} --conf spark.dynamicAllocation.enabled=true \
       --conf spark.dynamicAllocation.minExecutors=1 --conf spark.dynamicAllocation.maxExecutors=${NUM_EXECUTORS} \
       --conf spark.dynamicAllocation.executorIdleTimeout=60 --conf spark.shuffle.service.enabled=true"
      ;;
    *)
      SPARK_SUBMIT_CMD="${SPARK_SUBMIT_CMD} --num-executors=${NUM_EXECUTORS}"
      ;;
    esac
    SPARK_SUBMIT_CMD="${SPARK_SUBMIT_CMD} ${LAYER_JAR}"

    echo "${SPARK_SUBMIT_CMD}"
    bash -c "${SPARK_SUBMIT_CMD}"
    ;;

  yarn)
    # Launch layer in YARN

    LAYER_JAR_NAME=`basename ${LAYER_JAR}`
    FINAL_CLASSPATH="${LAYER_JAR_NAME}:${BASE_CLASSPATH}"
    if [ -n "${APP_JAR_NAME}" ]; then
      FINAL_CLASSPATH="${APP_JAR_NAME}:${FINAL_CLASSPATH}"
    fi

    LOCAL_SCRIPT_DIR="/tmp/${APP_NAME}"
    LOCAL_SCRIPT="${LOCAL_SCRIPT_DIR}/run-yarn.sh"
    YARN_LOG4J="${LOCAL_SCRIPT_DIR}/log4j.properties"
    # APP_NAME will match the base of what distributedshell uses, in the home dir
    HDFS_APP_DIR="${APP_NAME}"

    # Only one copy of the app can be running anyway, so fail if it already seems
    # to be running due to presence of directories
    if [ -d ${LOCAL_SCRIPT_DIR} ]; then
      usageAndExit "${LOCAL_SCRIPT_DIR} already exists; is ${APP_NAME} running?"
    fi
    if hdfs dfs -test -d ${HDFS_APP_DIR}; then
      usageAndExit "${HDFS_APP_DIR} already exists; is ${APP_NAME} running?"
    fi

    # Make temp directories to stage resources, locally and in HDFS
    mkdir -p ${LOCAL_SCRIPT_DIR}
    hdfs dfs -mkdir -p ${HDFS_APP_DIR}
    echo "Copying ${LAYER_JAR} and ${CONFIG_FILE} to ${HDFS_APP_DIR}/"
    hdfs dfs -put ${LAYER_JAR} ${CONFIG_FILE} ${HDFS_APP_DIR}/
    if [ -n "${APP_JAR}" ]; then
      echo "Copying ${APP_JAR} to ${HDFS_APP_DIR}/"
      hdfs dfs -put ${APP_JAR} ${HDFS_APP_DIR}/
    fi

    echo "log4j.logger.org.apache.hadoop.yarn.applications.distributedshell=WARN" >> ${YARN_LOG4J}

    # Need absolute path
    OWNER=`hdfs dfs -stat '%u' ${HDFS_APP_DIR}`
    echo "hdfs dfs -get /user/${OWNER}/${HDFS_APP_DIR}/* ." >> ${LOCAL_SCRIPT}
    echo "java ${JVM_ARGS} ${EXTRA_PROPS} -Dconfig.file=${CONFIG_FILE_NAME} -cp ${FINAL_CLASSPATH} ${MAIN_CLASS}" >> ${LOCAL_SCRIPT}

    YARN_DIST_SHELL_JAR=`bash ${COMPUTE_CLASSPATH} | grep distributedshell`

    echo "Running ${YARN_INSTANCES} ${APP_NAME} (${YARN_CORES} cores / ${MEMORY_MB}MB)"
    echo "Note that you will need to find the Application Master in YARN to find the Serving Layer"
    echo "instances, and kill the application with 'yarn application -kill [app ID]'"
    echo

    yarn jar ${YARN_DIST_SHELL_JAR} \
      -jar ${YARN_DIST_SHELL_JAR} \
      org.apache.hadoop.yarn.applications.distributedshell.Client \
      -appname ${APP_NAME} \
      -container_memory ${MEMORY_MB} \
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
    ;;

  *)
    # Launch Layer as local process

    FINAL_CLASSPATH="${LAYER_JAR}:${BASE_CLASSPATH}"
    if [ -n "${APP_JAR}" ]; then
      FINAL_CLASSPATH="${APP_JAR}:${FINAL_CLASSPATH}"
    fi
    java ${JVM_ARGS} ${EXTRA_PROPS} -Dconfig.file=${CONFIG_FILE} -cp ${FINAL_CLASSPATH} ${MAIN_CLASS}
    ;;

  esac
  ;;

kafka-setup|kafka-tail|kafka-input)

  setVarFromProperty "INPUT_ZK" "oryx\.input-topic\.lock\.master"
  setVarFromProperty "INPUT_KAFKA" "oryx\.input-topic\.broker"
  setVarFromProperty "INPUT_TOPIC" "oryx\.input-topic\.message\.topic"
  setVarFromProperty "UPDATE_ZK" "oryx\.update-topic\.lock\.master"
  setVarFromProperty "UPDATE_KAFKA" "oryx\.update-topic\.broker"
  setVarFromProperty "UPDATE_TOPIC" "oryx\.update-topic\.message\.topic"

  echo "Input   ZK      ${INPUT_ZK}"
  echo "        Kafka   ${INPUT_KAFKA}"
  echo "        topic   ${INPUT_TOPIC}"
  echo "Update  ZK      ${INPUT_ZK}"
  echo "        Kafka   ${UPDATE_KAFKA}"
  echo "        topic   ${UPDATE_TOPIC}"
  echo

  case "${COMMAND}" in
  kafka-setup)
    ALL_TOPICS=`${KAFKA_TOPICS_SH} --list --zookeeper ${INPUT_ZK}`
    echo "All available topics:"
    echo "${ALL_TOPICS}"
    echo

    if [ -z `echo "${ALL_TOPICS}" | grep ${INPUT_TOPIC}` ]; then
      read -p "Input topic ${INPUT_TOPIC} does not exist. Create it? " CREATE
      case "${CREATE}" in
        y|Y)
          echo "Creating topic ${INPUT_TOPIC}"
          ${KAFKA_TOPICS_SH} --zookeeper ${INPUT_ZK} --create --replication-factor 1 --partitions 4 --topic ${INPUT_TOPIC}
          ;;
      esac
    fi
    echo "Status of topic ${INPUT_TOPIC}:"
    ${KAFKA_TOPICS_SH} --zookeeper ${INPUT_ZK} --describe --topic ${INPUT_TOPIC}
    echo

    if [ -z `echo "${ALL_TOPICS}" | grep ${UPDATE_TOPIC}` ]; then
      read -p "Update topic ${UPDATE_TOPIC} does not exist. Create it? " CREATE
      case "${CREATE}" in
        y|Y)
          echo "Creating topic ${UPDATE_TOPIC}"
          ${KAFKA_TOPICS_SH} --zookeeper ${UPDATE_ZK} --create --replication-factor 1 --partitions 1 --topic ${UPDATE_TOPIC}
          ${KAFKA_TOPICS_SH} --zookeeper ${UPDATE_ZK} --alter --topic ${UPDATE_TOPIC} --config retention.ms=86400000 --config max.message.bytes=16777216
          ;;
      esac
    fi
    echo "Status of topic ${UPDATE_TOPIC}:"
    ${KAFKA_TOPICS_SH} --zookeeper ${UPDATE_ZK} --describe --topic ${UPDATE_TOPIC}
    echo
    ;;

  kafka-tail)
    ${KAFKA_CONSOLE_CONSUMER_SH} --zookeeper ${INPUT_ZK} --whitelist ${INPUT_TOPIC},${UPDATE_TOPIC} --property fetch.message.max.bytes=16777216
    ;;

  kafka-input)
    if [ ! -f "${INPUT_FILE}" ]; then
      usageAndExit "Input file ${INPUT_FILE} does not exist"
    fi
    ${KAFKA_CONSOLE_PRODUCER_SH} --broker-list ${INPUT_KAFKA} --topic ${INPUT_TOPIC} < "${INPUT_FILE}"
    ;;

  esac
  ;;

*)
  usageAndExit "Invalid command ${COMMAND}"
  ;;

esac
