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
  ls -1 /opt/cloudera/parcels/CDH/jars/$1 2>/dev/null | grep -vE "tests.jar$" | tail -1
}

# For Spark-based batch and speed layer, the only thing that needs to be supplied, really,
# are the Kafka libraries that the cluster uses. The Spark Examples jar happens to ship this
# and is maybe both easier to find and more harmonized than a stand-alone Kafka distro on
# the cluster, but this is a hacky way to acquire it
printLatest "spark-examples-*.jar"

# The remaining dependencies support the Serving Layer, which needs Hadoop, Kafka,
# and ZK dependencies
printLatest "spark-assembly-*.jar"
printLatest "zookeeper-*.jar"
printLatest "hadoop-auth-*.jar"
printLatest "hadoop-common-*.jar"
printLatest "hadoop-hdfs-2*.jar"
printLatest "htrace-core4-*.jar"
printLatest "commons-cli-1*.jar"
printLatest "commons-collections-*.jar"
printLatest "commons-configuration-*.jar"
printLatest "protobuf-java-2.5*.jar"
printLatest "snappy-java-*.jar"

# These are needed for submitting the serving layer in YARN mode
printLatest "hadoop-yarn-applications-distributedshell-*.jar"
