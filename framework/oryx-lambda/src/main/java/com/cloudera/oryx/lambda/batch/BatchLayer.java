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

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import kafka.serializer.Decoder;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.lang.ClassUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;

/**
 * Main entry point for Oryx Batch Layer.
 *
 * @param <K> type of key read from input topic
 * @param <M> type of message read from input topic
 * @param <U> type of model message written
 */
public final class BatchLayer<K,M,U> implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(BatchLayer.class);

  private final Config config;
  private final String id;
  private final String streamingMaster;
  private final String topicLockMaster;
  private final String messageTopic;
  private final Class<K> keyClass;
  private final Class<M> messageClass;
  private final Class<? extends Decoder<?>> keyDecoderClass;
  private final Class<? extends Decoder<?>> messageDecoderClass;
  private final Class<? extends Writable> keyWritableClass;
  private final Class<? extends Writable> messageWritableClass;
  private final String updateClassName;
  private final String dataDirString;
  private final String modelDirString;
  private final int numExecutors;
  private final int executorCores;
  private final String executorMemoryString;
  private final String driverMemoryString;
  private final int receiverParallelism;
  private final int generationIntervalSec;
  private final int blockIntervalSec;
  private final int storagePartitions;
  private final int uiPort;
  private JavaStreamingContext streamingContext;

  @SuppressWarnings("unchecked")
  public BatchLayer(Config config) {
    Preconditions.checkNotNull(config);
    log.info("Configuration:\n{}", ConfigUtils.prettyPrint(config));
    this.config = config;
    this.id = ConfigUtils.getOptionalString(config, "oryx.id");
    this.streamingMaster = config.getString("oryx.batch.streaming.master");
    this.topicLockMaster = config.getString("oryx.input-topic.lock.master");
    this.messageTopic = config.getString("oryx.input-topic.message.topic");
    this.keyClass = ClassUtils.loadClass(config.getString("oryx.input-topic.message.key-class"));
    this.messageClass =
        ClassUtils.loadClass(config.getString("oryx.input-topic.message.message-class"));
    this.keyDecoderClass = (Class<? extends Decoder<?>>) ClassUtils.loadClass(
        config.getString("oryx.input-topic.message.key-decoder-class"), Decoder.class);
    this.messageDecoderClass = (Class<? extends Decoder<?>>) ClassUtils.loadClass(
        config.getString("oryx.input-topic.message.message-decoder-class"), Decoder.class);
    this.keyWritableClass = ClassUtils.loadClass(
        config.getString("oryx.batch.storage.key-writable-class"), Writable.class);
    this.messageWritableClass = ClassUtils.loadClass(
        config.getString("oryx.batch.storage.message-writable-class"), Writable.class);
    this.updateClassName = config.getString("oryx.batch.update-class");
    this.dataDirString = config.getString("oryx.batch.storage.data-dir");
    this.modelDirString = config.getString("oryx.batch.storage.model-dir");
    this.numExecutors = config.getInt("oryx.batch.streaming.num-executors");
    this.executorCores = config.getInt("oryx.batch.streaming.executor-cores");
    this.executorMemoryString = config.getString("oryx.batch.streaming.executor-memory");
    this.driverMemoryString = config.getString("oryx.batch.streaming.driver-memory");
    this.receiverParallelism = config.getInt("oryx.batch.streaming.receiver-parallelism");
    this.generationIntervalSec = config.getInt("oryx.batch.streaming.generation-interval-sec");
    this.blockIntervalSec = config.getInt("oryx.batch.streaming.block-interval-sec");
    this.storagePartitions = config.getInt("oryx.batch.storage.partitions");
    this.uiPort = config.getInt("oryx.batch.ui.port");

    Preconditions.checkArgument(!dataDirString.isEmpty());
    Preconditions.checkArgument(!modelDirString.isEmpty());
    Preconditions.checkArgument(numExecutors >= 1);
    Preconditions.checkArgument(executorCores >= 1);
    Preconditions.checkArgument(receiverParallelism >= 1);
    Preconditions.checkArgument(generationIntervalSec > 0);
    Preconditions.checkArgument(blockIntervalSec > 0);
    Preconditions.checkArgument(storagePartitions > 0);
    Preconditions.checkArgument(uiPort > 0);
  }

  public synchronized void start() {
    if (id != null) {
      log.info("Starting Batch Layer {}", id);
    }
    log.info("Starting SparkContext for master {}, interval {} seconds",
             streamingMaster, generationIntervalSec);

    long blockIntervalMS = TimeUnit.MILLISECONDS.convert(blockIntervalSec, TimeUnit.SECONDS);

    SparkConf sparkConf = new SparkConf();

    sparkConf.setIfMissing("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

    sparkConf.setIfMissing("spark.executor.instances", Integer.toString(numExecutors));
    sparkConf.setIfMissing("spark.executor.cores", Integer.toString(executorCores));
    sparkConf.setIfMissing("spark.executor.memory", executorMemoryString);
    sparkConf.setIfMissing("spark.driver.memory", driverMemoryString);

    String blockIntervalString = Long.toString(blockIntervalMS);
    sparkConf.setIfMissing("spark.streaming.blockInterval", blockIntervalString);
    // Turn this down to prevent long blocking at shutdown
    sparkConf.setIfMissing("spark.streaming.gracefulStopTimeout", blockIntervalString);
    sparkConf.setIfMissing("spark.cleaner.ttl", Integer.toString(20 * generationIntervalSec));
    sparkConf.setIfMissing("spark.logConf", "true");
    sparkConf.setIfMissing("spark.ui.port", Integer.toString(uiPort));
    sparkConf.setIfMissing("spark.ui.showConsoleProgress", "false");

    // Want to use some versions that may not match Spark or YARN
    sparkConf.setIfMissing("spark.yarn.user.classpath.first", "true");
    // Don't set spark.files.userClassPathFirst (see issue #120)

    sparkConf.setMaster(streamingMaster);
    sparkConf.setAppName("OryxBatchLayer");
    long batchDurationMS =
       TimeUnit.MILLISECONDS.convert(generationIntervalSec, TimeUnit.SECONDS);

    streamingContext = new JavaStreamingContext(new JavaSparkContext(sparkConf),
                                                new Duration(batchDurationMS));

    log.info("Creating message stream from topic");

    JavaPairDStream<K,M> dStream = buildDStream();

    dStream.foreachRDD(
        new BatchUpdateFunction<>(config,
                                  keyClass,
                                  messageClass,
                                  keyWritableClass,
                                  messageWritableClass,
                                  dataDirString,
                                  modelDirString,
                                  loadUpdateInstance(),
                                  streamingContext));

    // Save data to HDFS. Write the original message type, not transformed.
    JavaPairDStream<Writable,Writable> writableDStream =
        dStream.repartition(storagePartitions).mapToPair(
            new ValueToWritableFunction<>(keyClass,
                                          messageClass,
                                          keyWritableClass,
                                          messageWritableClass));

    // This horrible, separate declaration is necessary to appease the compiler
    @SuppressWarnings("unchecked")
    Class<? extends OutputFormat<?,?>> outputFormatClass =
        (Class<? extends OutputFormat<?,?>>) (Class<?>) SequenceFileOutputFormat.class;

    // "Inline" saveAsNewAPIHadoopFiles to be able to skip saving empty RDDs
    writableDStream.foreachRDD(
        new SaveToHDFSFunction(dataDirString + "/oryx",
                               "data",
                               keyWritableClass,
                               messageWritableClass,
                               outputFormatClass,
                               streamingContext.sparkContext().hadoopConfiguration()));

    log.info("Starting Spark Streaming");

    streamingContext.start();
  }

  public void await() {
    Preconditions.checkState(streamingContext != null);
    log.info("Spark Streaming is running");
    streamingContext.awaitTermination();
  }

  @Override
  public synchronized void close() {
    if (streamingContext != null) {
      log.info("Shutting down");
      streamingContext.stop(true, true);
      streamingContext = null;
    }
  }

  private JavaPairDStream<K,M> buildDStream() {
    Map<String,String> kafkaParams = new HashMap<>();
    kafkaParams.put("zookeeper.connect", topicLockMaster);
    kafkaParams.put("group.id", "OryxGroup-BatchLayer-" + System.currentTimeMillis());
    // Don't re-consume old messages from input
    kafkaParams.put("auto.offset.reset", "largest");
    if (id != null) {
      // Set consumer.id to access last read offset for the layer
      kafkaParams.put("consumer.id", "Oryx-BatchLayer-" + id);
    }

    List<JavaPairDStream<K,M>> streams = new ArrayList<>(receiverParallelism);
    for (int i = 0; i < receiverParallelism; i++) {
      streams.add(KafkaUtils.createStream(
                    streamingContext,
                    keyClass,
                    messageClass,
                    keyDecoderClass,
                    messageDecoderClass,
                    kafkaParams,
                    Collections.singletonMap(messageTopic, 1),
                    StorageLevel.MEMORY_AND_DISK_2()));
    }

    if (streams.size() == 1) {
      return streams.get(0);
    } else {
      return streamingContext.union(streams.get(0), streams.subList(1, streams.size()));
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
            new Class<?>[]{Config.class},
            new Object[]{config});
      } catch (IllegalArgumentException iae) {
        return ClassUtils.loadInstanceOf(updateClassName, BatchLayerUpdate.class);
      }

    } else if (ScalaBatchLayerUpdate.class.isAssignableFrom(updateClass)) {

      try {
        return new ScalaBatchLayerUpdateAdapter<>(ClassUtils.loadInstanceOf(
            updateClassName,
            ScalaBatchLayerUpdate.class,
            new Class<?>[]{Config.class},
            new Object[]{config}));
      } catch (IllegalArgumentException iae) {
        return new ScalaBatchLayerUpdateAdapter<>(ClassUtils.loadInstanceOf(
            updateClassName, ScalaBatchLayerUpdate.class));
      }

    } else {
      throw new IllegalArgumentException("Bad update class: " + updateClassName);
    }
  }

}
