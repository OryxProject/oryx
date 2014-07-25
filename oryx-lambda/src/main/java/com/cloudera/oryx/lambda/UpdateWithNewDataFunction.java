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

package com.cloudera.oryx.lambda;

import java.io.IOException;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.lambda.update.BatchLayerUpdate;

/**
 * Framework for executing the computation layer update, and storing data to persistent storage,
 * in the context of a streaming framework.
 *
 * @param <K> type of key read from input queue
 * @param <M> type of message read from input queue
 * @param <O> type of model message written
 */
final class UpdateWithNewDataFunction<K,M,O> implements Function2<JavaPairRDD<K,M>,Time,Void> {

  private static final Logger log = LoggerFactory.getLogger(UpdateWithNewDataFunction.class);

  private final Class<K> keyClass;
  private final Class<M> messageClass;
  private final String dataDirString;
  private final String modelDirString;
  private final BatchSerializationConfig batchSerializationConfig;
  private final BatchLayerUpdate<K,M,O> updateInstance;
  private final String updateBroker;
  private final String updateTopic;

  // Can't be serialized, but shouldn't need to be (?)
  private final transient JavaSparkContext sparkContext;

  UpdateWithNewDataFunction(Config config,
                            Class<K> keyClass,
                            Class<M> messageClass,
                            String dataDirString,
                            String modelDirString,
                            BatchSerializationConfig batchSerializationConfig,
                            BatchLayerUpdate<K,M,O> updateInstance,
                            JavaStreamingContext streamingContext) {
    this.keyClass = keyClass;
    this.messageClass = messageClass;
    this.dataDirString = dataDirString;
    this.modelDirString = modelDirString;
    this.batchSerializationConfig = batchSerializationConfig;
    this.updateBroker = config.getString("update-queue.broker");
    this.updateTopic = config.getString("update-queue.message.topic");
    this.updateInstance = updateInstance;
    this.sparkContext = streamingContext.sparkContext();
  }

  @Override
  public Void call(JavaPairRDD<K,M> newData, Time timestamp)
      throws IOException, InterruptedException {

    Preconditions.checkNotNull(sparkContext, "SparkConfiguration required but not available");
    Configuration hadoopConf = sparkContext.hadoopConfiguration();

    JavaPairRDD<K,M> pastData;
    Path inputPathPattern = new Path(dataDirString + "/*/part-*");
    FileSystem fs = FileSystem.get(hadoopConf);
    FileStatus[] inputPathStatuses = fs.globStatus(inputPathPattern);
    if (inputPathStatuses == null || inputPathStatuses.length == 0) {

      log.info("No past data at path(s) {}", inputPathPattern);
      pastData = null;

    } else {

      log.info("Found past data at path(s) like {} , ...", inputPathStatuses[0].getPath());
      Configuration updatedConf = new Configuration(hadoopConf);
      updatedConf.set(FileInputFormat.INPUT_DIR, joinFSPaths(fs, inputPathStatuses));

      @SuppressWarnings("unchecked")
      JavaPairRDD<Writable,Writable> pastWritableData = (JavaPairRDD<Writable,Writable>)
          sparkContext.newAPIHadoopRDD(updatedConf,
                                       SequenceFileInputFormat.class,
                                       batchSerializationConfig.getKeyWritableClass(),
                                       batchSerializationConfig.getMessageWritableClass());

      pastData = pastWritableData.mapToPair(
          new WritableToValueFunction<>(keyClass,
                                        messageClass,
                                        batchSerializationConfig));
    }

    try (QueueProducer<String,O> producer = new QueueProducer<>(updateBroker, updateTopic)) {
      updateInstance.configureUpdate(sparkContext,
                                     timestamp.milliseconds(),
                                     newData,
                                     pastData,
                                     modelDirString,
                                     producer);
    }

    return null;
  }

  /**
   * @return paths from {@link FileStatus}es into one comma-separated String
   * @see FileInputFormat#addInputPath(org.apache.hadoop.mapreduce.Job, Path)
   */
  private static String joinFSPaths(FileSystem fs, FileStatus[] statuses) {
    StringBuilder joined = new StringBuilder();
    for (FileStatus status : statuses) {
      if (joined.length() > 0) {
        joined.append(',');
      }
      Path path = fs.makeQualified(status.getPath());
      joined.append(StringUtils.escapeString(path.toString()));
    }
    return joined.toString();
  }

}
