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

# CDH5.2+ specific classpath config

HADOOP_CONF_DIR="/etc/hadoop/conf"
CDH_JARS_DIR="/opt/cloudera/parcels/CDH/jars"

echo ${HADOOP_CONF_DIR}
ls -1 \
 ${CDH_JARS_DIR}/zookeeper-*.jar \
 ${CDH_JARS_DIR}/spark-assembly-*.jar \
 ${CDH_JARS_DIR}/spark-examples-*.jar \
 ${CDH_JARS_DIR}/hadoop-auth-*.jar \
 ${CDH_JARS_DIR}/hadoop-common-*.jar \
 ${CDH_JARS_DIR}/hadoop-hdfs-*.jar \
 ${CDH_JARS_DIR}/hadoop-mapreduce-client-core-*.jar \
 ${CDH_JARS_DIR}/hadoop-yarn-api-*.jar \
 ${CDH_JARS_DIR}/hadoop-yarn-client-*.jar \
 ${CDH_JARS_DIR}/hadoop-yarn-common-*.jar \
 ${CDH_JARS_DIR}/hadoop-yarn-server-web-proxy-*.jar \
 ${CDH_JARS_DIR}/commons-cli-*.jar \
 ${CDH_JARS_DIR}/commons-collections-*.jar \
 ${CDH_JARS_DIR}/commons-configuration-*.jar \
 ${CDH_JARS_DIR}/commons-lang-*.jar \
 ${CDH_JARS_DIR}/protobuf-java-*.jar \
 | grep -E "[0-9]\\.jar$"
