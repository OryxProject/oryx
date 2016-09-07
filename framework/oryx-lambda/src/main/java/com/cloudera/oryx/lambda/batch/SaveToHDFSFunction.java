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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Function that saves RDDs to HDFS -- only if they're non empty, to prevent creation
 * of many small empty files if data is infrequent but the model interval is short.
 */
final class SaveToHDFSFunction<K,M> implements VoidFunction2<JavaPairRDD<K,M>,Time> {

  private static final Logger log = LoggerFactory.getLogger(SaveToHDFSFunction.class);

  private final String prefix;
  private final String suffix;
  private final Class<K> keyClass;
  private final Class<M> messageClass;
  private final Class<? extends Writable> keyWritableClass;
  private final Class<? extends Writable> messageWritableClass;
  private final Configuration hadoopConf;

  SaveToHDFSFunction(String prefix,
                     String suffix,
                     Class<K> keyClass,
                     Class<M> messageClass,
                     Class<? extends Writable> keyWritableClass,
                     Class<? extends Writable> messageWritableClass,
                     Configuration hadoopConf) {
    this.prefix = prefix;
    this.suffix = suffix;
    this.keyClass = keyClass;
    this.messageClass = messageClass;
    this.keyWritableClass = keyWritableClass;
    this.messageWritableClass = messageWritableClass;
    this.hadoopConf = hadoopConf;
  }

  @Override
  public void call(JavaPairRDD<K,M> rdd, Time time) throws IOException {
    if (rdd.isEmpty()) {
      log.info("RDD was empty, not saving to HDFS");
    } else {
      String file = prefix + "-" + time.milliseconds() + "." + suffix;
      Path path = new Path(file);
      FileSystem fs = FileSystem.get(hadoopConf);
      if (fs.exists(path)) {
        log.warn("Saved data already existed, possibly from a failed job. Deleting {}", path);
        fs.delete(path, true);
      }
      log.info("Saving RDD to HDFS at {}", file);
      rdd.mapToPair(
          new ValueToWritableFunction<>(keyClass, messageClass, keyWritableClass, messageWritableClass)
      ).saveAsNewAPIHadoopFile(
          file,
          keyWritableClass,
          messageWritableClass,
          SequenceFileOutputFormat.class,
          hadoopConf);
    }
  }
}
