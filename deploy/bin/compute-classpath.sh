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

# This is a hacky means to plug in to the versions of libraries used on the cluster
# rather than ship a particular version with the binaries.
# TODO: we need a better solution to dependencies

# CDH specific classpath config

function printLatest() {
  ls -1 /opt/cloudera/parcels/$1 2>/dev/null | grep -vE "(tests|sources).jar$" | tail -1
}

# This only supports the Serving Layer, which needs Hadoop, Kafka, and ZK dependencies
printLatest "SPARK2/lib/spark2/jars/scala-library-*.jar"
printLatest "SPARK2/lib/spark2/jars/scala-parser-*.jar"
printLatest "KAFKA/lib/kafka/libs/kafka_*.jar" 
printLatest "KAFKA/lib/kafka/libs/kafka-clients*.jar" 
printLatest "KAFKA/lib/kafka/libs/metrics-core*.jar" 
printLatest "KAFKA/lib/kafka/libs/zkclient-*.jar" 
printLatest "CDH/jars/zookeeper-*.jar"
printLatest "CDH/jars/hadoop-auth-*.jar"
printLatest "CDH/jars/hadoop-common-*.jar"
printLatest "CDH/jars/hadoop-hdfs-2*.jar"
printLatest "CDH/jars/commons-cli-1*.jar"
printLatest "CDH/jars/commons-collections-*.jar"
printLatest "CDH/jars/commons-configuration-*.jar"
printLatest "CDH/jars/protobuf-java-*.jar"
printLatest "CDH/jars/snappy-java-*.jar"

# These are needed for submitting the serving layer in YARN mode
printLatest "CDH/jars/hadoop-yarn-applications-distributedshell-*.jar"
