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

# CDH5.4+ specific classpath config

HADOOP_CONF_DIR="/etc/hadoop/conf"
CDH_JARS_DIR="/opt/cloudera/parcels/CDH/jars"

function printLatest() {
  ls -1 ${CDH_JARS_DIR}/$1 | grep -E "[0-9]\\.jar$" | tail -1
}

# Some are known to have multiple versions, and so are fully specified:

echo ${HADOOP_CONF_DIR}
printLatest "zookeeper-*.jar"
printLatest "spark-assembly-*.jar"
printLatest "spark-examples-*.jar"
printLatest "hadoop-auth-*.jar"
printLatest "hadoop-common-*.jar"
printLatest "hadoop-hdfs-2*.jar"
printLatest "hadoop-mapreduce-client-core-*.jar"
printLatest "hadoop-yarn-api-*.jar"
printLatest "hadoop-yarn-client-*.jar"
printLatest "hadoop-yarn-common-*.jar"
printLatest "hadoop-yarn-server-web-proxy-*.jar"
printLatest "hadoop-yarn-applications-distributedshell-*.jar"
printLatest "htrace-core-3.*.jar"
printLatest "commons-cli-1.*.jar"
printLatest "commons-collections-*.jar"
printLatest "commons-configuration-1.*.jar"
printLatest "commons-lang-2.*.jar"
printLatest "httpclient-4.2.*.jar"
printLatest "httpcore-4.2.*.jar"
printLatest "httpmime-4.2.*.jar"
printLatest "jackson-core-asl-1.9.12.jar"
printLatest "jackson-mapper-asl-1.9.12.jar"
printLatest "protobuf-java-*.jar"
printLatest "snappy-java-*.jar"
printLatest "avro-1.7.*.jar"
