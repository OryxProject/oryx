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

import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import kafka.message.MessageAndMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import com.cloudera.oryx.api.batch.BatchLayerUpdate;
import com.cloudera.oryx.api.batch.ScalaBatchLayerUpdate;
import com.cloudera.oryx.common.lang.ClassUtils;
import com.cloudera.oryx.lambda.AbstractSparkLayer;
import com.cloudera.oryx.lambda.DeleteOldDataFn;
import com.cloudera.oryx.lambda.UpdateOffsetsFn;

/**
 * Main entry point for Oryx Batch Layer.
 *
 * @param <K> type of key read from input topic
 * @param <M> type of message read from input topic
 * @param <U> type of model message written
 */
public final class BatchLayer<K,M,U> extends AbstractSparkLayer<K,M> {

  private static final Logger log = LoggerFactory.getLogger(BatchLayer.class);

  private static final int NO_MAX_AGE = -1;

  private final Class<? extends Writable> keyWritableClass;
  private final Class<? extends Writable> messageWritableClass;
  private final String updateClassName;
  private final String dataDirString;
  private final String modelDirString;
  private final int maxDataAgeHours;
  private final int maxModelAgeHours;
  private JavaStreamingContext streamingContext;

  public BatchLayer(Config config) {
    super(config);
    this.keyWritableClass = ClassUtils.loadClass(
        config.getString("oryx.batch.storage.key-writable-class"), Writable.class);
    this.messageWritableClass = ClassUtils.loadClass(
        config.getString("oryx.batch.storage.message-writable-class"), Writable.class);
    this.updateClassName = config.getString("oryx.batch.update-class");
    this.dataDirString = config.getString("oryx.batch.storage.data-dir");
    this.modelDirString = config.getString("oryx.batch.storage.model-dir");
    this.maxDataAgeHours = config.getInt("oryx.batch.storage.max-age-data-hours");
    this.maxModelAgeHours = config.getInt("oryx.batch.storage.max-age-model-hours");
    Preconditions.checkArgument(!dataDirString.isEmpty());
    Preconditions.checkArgument(!modelDirString.isEmpty());
    Preconditions.checkArgument(maxDataAgeHours >= 0 || maxDataAgeHours == NO_MAX_AGE);
    Preconditions.checkArgument(maxModelAgeHours >= 0 || maxModelAgeHours == NO_MAX_AGE);
  }

  @Override
  protected String getConfigGroup() {
    return "batch";
  }

  @Override
  protected String getLayerName() {
    return "BatchLayer";
  }

  public synchronized void start() {
    String id = getID();
    if (id != null) {
      log.info("Starting Batch Layer {}", id);
    }

    streamingContext = buildStreamingContext();
    JavaSparkContext sparkContext = streamingContext.sparkContext();
    Configuration hadoopConf = sparkContext.hadoopConfiguration();

    Path checkpointPath = new Path(new Path(modelDirString), ".checkpoint");
    log.info("Setting checkpoint dir to {}", checkpointPath);
    sparkContext.setCheckpointDir(checkpointPath.toString());

    log.info("Creating message stream from topic");
    JavaInputDStream<MessageAndMetadata<K,M>> kafkaDStream = buildInputDStream(streamingContext);
    JavaPairDStream<K,M> pairDStream =
        kafkaDStream.mapToPair(mAndM -> new Tuple2<>(mAndM.key(), mAndM.message()));

    Class<K> keyClass = getKeyClass();
    Class<M> messageClass = getMessageClass();
    pairDStream.foreachRDD(
        new BatchUpdateFunction<>(getConfig(),
                                  keyClass,
                                  messageClass,
                                  keyWritableClass,
                                  messageWritableClass,
                                  dataDirString,
                                  modelDirString,
                                  loadUpdateInstance(),
                                  streamingContext));

    // "Inline" saveAsNewAPIHadoopFiles to be able to skip saving empty RDDs
    pairDStream.foreachRDD(new SaveToHDFSFunction<>(
        dataDirString + "/oryx",
        "data",
        keyClass,
        messageClass,
        keyWritableClass,
        messageWritableClass,
        hadoopConf));

    // Must use the raw Kafka stream to get offsets
    kafkaDStream.foreachRDD(new UpdateOffsetsFn<>(getGroupID(), getInputTopicLockMaster()));

    if (maxDataAgeHours != NO_MAX_AGE) {
      pairDStream.foreachRDD(new DeleteOldDataFn<>(hadoopConf,
                                                   dataDirString,
                                                   Pattern.compile("-(\\d+)\\."),
                                                   maxDataAgeHours));
    }
    if (maxModelAgeHours != NO_MAX_AGE) {
      pairDStream.foreachRDD(new DeleteOldDataFn<>(hadoopConf,
                                                   modelDirString,
                                                   Pattern.compile("(\\d+)"),
                                                   maxModelAgeHours));
    }

    log.info("Starting Spark Streaming");

    streamingContext.start();
  }

  public void await() throws InterruptedException {
    JavaStreamingContext theStreamingContext;
    synchronized (this) {
      theStreamingContext = streamingContext;
      Preconditions.checkState(theStreamingContext != null);
    }
    log.info("Spark Streaming is running");
    theStreamingContext.awaitTermination(); // Can't do this with lock
  }

  @Override
  public synchronized void close() {
    if (streamingContext != null) {
      log.info("Shutting down Spark Streaming; this may take some time");
      streamingContext.stop(true, true);
      streamingContext = null;
    }
  }

  @SuppressWarnings("unchecked")
  private BatchLayerUpdate<K,M,U> loadUpdateInstance() {
    Class<?> updateClass = ClassUtils.loadClass(updateClassName);

    if (BatchLayerUpdate.class.isAssignableFrom(updateClass)) {

      try {
        return ClassUtils.loadInstanceOf(
            updateClassName,
            BatchLayerUpdate.class,
            new Class<?>[] { Config.class },
            new Object[] { getConfig() });
      } catch (IllegalArgumentException iae) {
        return ClassUtils.loadInstanceOf(updateClassName, BatchLayerUpdate.class);
      }

    } else if (ScalaBatchLayerUpdate.class.isAssignableFrom(updateClass)) {

      try {
        return new ScalaBatchLayerUpdateAdapter<>(ClassUtils.loadInstanceOf(
            updateClassName,
            ScalaBatchLayerUpdate.class,
            new Class<?>[] { Config.class },
            new Object[] { getConfig() }));
      } catch (IllegalArgumentException iae) {
        return new ScalaBatchLayerUpdateAdapter<>(ClassUtils.loadInstanceOf(
            updateClassName, ScalaBatchLayerUpdate.class));
      }

    } else {
      throw new IllegalArgumentException("Bad update class: " + updateClassName);
    }
  }

}
