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

import java.io.Closeable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.lang.ClassUtils;

/**
 * Main entry point for Oryx Batch Layer.
 *
 * @param <K> type of key read from input queue
 * @param <M> type of message read from input queue
 * @param <U> type of model message written
 */
public final class BatchLayer<K,M,U> implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(BatchLayer.class);

  private final Config config;
  private final String streamingMaster;
  private final String queueLockMaster;
  private final String messageTopic;
  private final Class<K> keyClass;
  private final Class<M> messageClass;
  private final String updateClassName;
  private final String dataDirString;
  private final String modelDirString;
  private final int generationIntervalSec;
  private final int blockIntervalSec;
  private final int storagePartitions;
  private final InputSerializationConfig inputSerializationConfig;
  private JavaStreamingContext streamingContext;

  public BatchLayer(Config config) {
    Preconditions.checkNotNull(config);
    this.config = config;
    this.streamingMaster = config.getString("batch.streaming.master");
    this.queueLockMaster = config.getString("input-queue.lock.master");
    this.messageTopic = config.getString("input-queue.message.topic");
    this.keyClass = ClassUtils.loadClass(config.getString("input-queue.message.key-class"));
    this.messageClass = ClassUtils.loadClass(config.getString("input-queue.message.message-class"));
    this.updateClassName = config.getString("batch.update-class");
    this.dataDirString = config.getString("batch.storage.data-dir");
    this.modelDirString = config.getString("batch.storage.model-dir");
    this.generationIntervalSec = config.getInt("batch.generation-interval-sec");
    this.blockIntervalSec = config.getInt("batch.block-interval-sec");
    this.storagePartitions = config.getInt("batch.storage.partitions");
    this.inputSerializationConfig = new InputSerializationConfig(config);

    Preconditions.checkArgument(generationIntervalSec > 0);
    Preconditions.checkArgument(blockIntervalSec > 0);
    Preconditions.checkArgument(storagePartitions > 0);
  }

  public synchronized void start() {
    log.info("Starting SparkContext for master {}, interval {} seconds",
             streamingMaster, generationIntervalSec);

    long blockIntervalMS = TimeUnit.MILLISECONDS.convert(blockIntervalSec, TimeUnit.SECONDS);

    SparkConf sparkConf = new SparkConf();
    sparkConf.setIfMissing("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.setIfMissing("spark.streaming.blockInterval", Long.toString(blockIntervalMS));
    sparkConf.setIfMissing("spark.cleaner.ttl", Integer.toString(3 * generationIntervalSec));
    sparkConf.setIfMissing("spark.logConf", "true");
    sparkConf.setMaster(streamingMaster);
    sparkConf.setAppName("OryxBatchLayer");
    long batchDurationMS = TimeUnit.MILLISECONDS.convert(generationIntervalSec, TimeUnit.SECONDS);
    streamingContext = new JavaStreamingContext(sparkConf, new Duration(batchDurationMS));

    log.info("Creating message queue stream");

    JavaPairDStream<K,M> dStream = buildDStream();

    dStream.foreachRDD(
        new BatchUpdateFunction<>(config,
                                  keyClass,
                                  messageClass,
                                  dataDirString,
                                  modelDirString,
            inputSerializationConfig,
                                  loadUpdateInstance(),
                                  streamingContext));

    // Save data to HDFS. Write the original message type, not transformed.
    JavaPairDStream<Writable,Writable> writableDStream =
        dStream.repartition(storagePartitions).mapToPair(
            new ValueToWritableFunction<>(keyClass, messageClass, inputSerializationConfig));

    // This horrible, separate declaration is necessary to appease the compiler
    @SuppressWarnings("unchecked")
    Class<? extends OutputFormat<?,?>> outputFormatClass =
        (Class<? extends OutputFormat<?,?>>) (Class<?>) SequenceFileOutputFormat.class;
    writableDStream.saveAsNewAPIHadoopFiles(dataDirString + "/oryx",
                                            "data",
                                            inputSerializationConfig.getKeyWritableClass(),
                                            inputSerializationConfig.getMessageWritableClass(),
                                            outputFormatClass,
                                            streamingContext.sparkContext().hadoopConfiguration());

    log.info("Starting streaming");

    streamingContext.start();
  }

  public void await() {
    Preconditions.checkState(streamingContext != null);
    log.info("Waiting for streaming...");
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
    kafkaParams.put("zookeeper.connect", queueLockMaster);
    kafkaParams.put("group.id", "OryxGroup-BatchLayer-" + System.currentTimeMillis());
    return KafkaUtils.createStream(
        streamingContext,
        keyClass,
        messageClass,
        inputSerializationConfig.getKeyDecoderClass(),
        inputSerializationConfig.getMessageDecoderClass(),
        kafkaParams,
        Collections.singletonMap(messageTopic, 1),
        StorageLevel.MEMORY_AND_DISK_2());
  }

  private BatchLayerUpdate<K,M,U> loadUpdateInstance() {
    Class<?> updateClass = ClassUtils.loadClass(updateClassName);

    if (BatchLayerUpdate.class.isAssignableFrom(updateClass)) {

      try {
        @SuppressWarnings("unchecked")
        BatchLayerUpdate<K,M,U> instance = ClassUtils.loadInstanceOf(
            updateClassName,
            BatchLayerUpdate.class,
            new Class<?>[]{Config.class},
            new Object[]{config});
        return instance;

      } catch (IllegalArgumentException iae) {
        @SuppressWarnings("unchecked")
        BatchLayerUpdate<K,M,U> instance =
            ClassUtils.loadInstanceOf(updateClassName, BatchLayerUpdate.class);
        return instance;
      }

    } else if (ScalaBatchLayerUpdate.class.isAssignableFrom(updateClass)) {

      try {
        @SuppressWarnings("unchecked")
        ScalaBatchLayerUpdate<K,M,U> instance = ClassUtils.loadInstanceOf(
              updateClassName,
            ScalaBatchLayerUpdate.class,
              new Class<?>[]{Config.class},
              new Object[]{config});
        return new ScalaBatchLayerUpdateAdapter<>(instance);

      } catch (IllegalArgumentException iae) {
        @SuppressWarnings("unchecked")
        ScalaBatchLayerUpdate<K,M,U> instance =
            ClassUtils.loadInstanceOf(updateClassName, ScalaBatchLayerUpdate.class);
        return new ScalaBatchLayerUpdateAdapter<>(instance);
      }

    } else {
      throw new IllegalArgumentException("Bad update class: " + updateClassName);
    }
  }

}
