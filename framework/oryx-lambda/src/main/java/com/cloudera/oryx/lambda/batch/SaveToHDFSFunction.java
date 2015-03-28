/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.oryx.lambda.batch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Function that saves RDDs to HDFS -- only if they're non empty, to prevent creation
 * of many small empty files if data is infrequent but the model interval is short.
 */
final class SaveToHDFSFunction
    implements Function2<JavaPairRDD<Writable,Writable>,Time,Void> {

  private static final Logger log = LoggerFactory.getLogger(SaveToHDFSFunction.class);

  private final String prefix;
  private final String suffix;
  private final Class<? extends Writable> keyWritableClass;
  private final Class<? extends Writable> messageWritableClass;
  private final Configuration hadoopConf;

  SaveToHDFSFunction(String prefix,
                     String suffix,
                     Class<? extends Writable> keyWritableClass,
                     Class<? extends Writable> messageWritableClass,
                     Configuration hadoopConf) {
    this.prefix = prefix;
    this.suffix = suffix;
    this.keyWritableClass = keyWritableClass;
    this.messageWritableClass = messageWritableClass;
    this.hadoopConf = hadoopConf;
  }

  @Override
  public Void call(JavaPairRDD<Writable, Writable> rdd, Time time) {
    if (rdd.isEmpty()) {
      log.info("RDD was empty, not saving to HDFS");
    } else {
      String file = prefix + "-" + time.milliseconds() + "." + suffix;
      log.info("Saving RDD to HDFS at {}", file);
      rdd.saveAsNewAPIHadoopFile(file,
                                 keyWritableClass,
                                 messageWritableClass,
                                 SequenceFileOutputFormat.class,
                                 hadoopConf);
    }
    return null;
  }
}
