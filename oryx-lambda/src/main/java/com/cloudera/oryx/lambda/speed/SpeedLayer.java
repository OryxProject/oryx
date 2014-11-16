/*
 * Copyright (c) 2014, Cloudera, Inc. and Intel Corp. All Rights Reserved.
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

package com.cloudera.oryx.lambda.speed;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.typesafe.config.Config;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.lang.ClassUtils;
import com.cloudera.oryx.common.lang.LoggingRunnable;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.lambda.KeyMessage;

/**
 * Main entry point for Oryx Speed Layer.
 *
 * @param <K> type of key read from input queue
 * @param <M> type of message read from input queue
 * @param <U> type of update message read/written
 */
public final class SpeedLayer<K,M,U> implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(SpeedLayer.class);

  private final Config config;
  private final String streamingMaster;
  private final String inputQueueLockMaster;
  private final String messageTopic;
  private final String updateBroker;
  private final String updateTopic;
  private final String updateQueueLockMaster;
  private final String modelManagerClassName;
  private final String checkpointDirString;
  private final int generationIntervalSec;
  private final int blockIntervalSec;
  private final Class<? extends Decoder<?>> keyDecoderClass;
  private final Class<? extends Decoder<?>> messageDecoderClass;
  private final Class<? extends Decoder<U>> updateDecoderClass;
  private JavaStreamingContext streamingContext;
  private ConsumerConnector consumer;
  private SpeedModelManager<K,M,U> modelManager;
  private final Class<K> keyClass;
  private final Class<M> messageClass;

  @SuppressWarnings("unchecked")
  public SpeedLayer(Config config) {
    Preconditions.checkNotNull(config);
    log.info("Configuration:\n{}", ConfigUtils.prettyPrint(config));
    this.config = config;
    this.streamingMaster = config.getString("speed.streaming.master");
    this.inputQueueLockMaster = config.getString("input-queue.lock.master");
    this.messageTopic = config.getString("input-queue.message.topic");
    this.updateBroker = config.getString("update-queue.broker");
    this.updateTopic = config.getString("update-queue.message.topic");
    this.updateQueueLockMaster = config.getString("update-queue.lock.master");
    this.modelManagerClassName = config.getString("speed.model-manager-class");
    this.checkpointDirString = config.hasPath("speed.storage.checkpoint-dir") ?
        config.getString("speed.storage.checkpoint-dir") :
        null;
    this.generationIntervalSec = config.getInt("speed.generation-interval-sec");
    this.blockIntervalSec = config.getInt("speed.block-interval-sec");
    this.keyDecoderClass = (Class<? extends Decoder<?>>) ClassUtils.loadClass(
        config.getString("input-queue.message.key-decoder-class"), Decoder.class);
    this.messageDecoderClass = (Class<? extends Decoder<?>>) ClassUtils.loadClass(
        config.getString("input-queue.message.message-decoder-class"), Decoder.class);
    this.updateDecoderClass = (Class<? extends Decoder<U>>) ClassUtils.loadClass(
        config.getString("update-queue.message.decoder-class"), Decoder.class);
    this.keyClass = ClassUtils.loadClass(config.getString("input-queue.message.key-class"));
    this.messageClass = ClassUtils.loadClass(config.getString("input-queue.message.message-class"));

    Preconditions.checkArgument(this.generationIntervalSec > 0);
    Preconditions.checkArgument(this.blockIntervalSec > 0);
  }

  public synchronized void start() {
    log.info("Starting SparkContext for master {}, interval {} seconds",
             streamingMaster, generationIntervalSec);

    long blockIntervalMS = TimeUnit.MILLISECONDS.convert(blockIntervalSec, TimeUnit.SECONDS);

    SparkConf sparkConf = new SparkConf();
    sparkConf.setIfMissing("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.setIfMissing("spark.streaming.blockInterval", Long.toString(blockIntervalMS));
    sparkConf.setIfMissing("spark.cleaner.ttl", Integer.toString(20 * generationIntervalSec));
    sparkConf.setIfMissing("spark.logConf", "true");
    sparkConf.setMaster(streamingMaster);
    sparkConf.setAppName("OryxSpeedLayer");
    final long batchDurationMS =
        TimeUnit.MILLISECONDS.convert(generationIntervalSec, TimeUnit.SECONDS);
    final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
    Configuration hadoopConf = sparkContext.hadoopConfiguration();

    if (checkpointDirString == null) {
      log.info("Not using a streaming checkpoint dir");
      streamingContext = new JavaStreamingContext(sparkContext, new Duration(batchDurationMS));
    } else {
      log.info("Using streaming checkpoint dir {}", checkpointDirString);
      JavaStreamingContextFactory streamingContextFactory = new JavaStreamingContextFactory() {
        @Override
        public JavaStreamingContext create() {
          JavaStreamingContext jssc =
              new JavaStreamingContext(sparkContext, new Duration(batchDurationMS));
          jssc.checkpoint(checkpointDirString);
          return jssc;
        }
      };
      streamingContext = JavaStreamingContext.getOrCreate(
          checkpointDirString, hadoopConf, streamingContextFactory, false);
      streamingContext.checkpoint(checkpointDirString);
    }

    log.info("Creating message queue stream");

    JavaPairDStream<K,M> dStream = buildDStream();

    Properties consumerProps = new Properties();
    consumerProps.setProperty("group.id", "OryxGroup-SpeedLayer-" + System.currentTimeMillis());
    consumerProps.setProperty("zookeeper.connect", updateQueueLockMaster);
    ConsumerConfig consumerConfig = new ConsumerConfig(consumerProps);
    consumer = Consumer.createJavaConsumerConnector(consumerConfig);
    KafkaStream<String,U> stream =
        consumer.createMessageStreams(Collections.singletonMap(updateTopic, 1),
                                      new StringDecoder(null),
                                      loadDecoderInstance())
            .get(updateTopic).get(0);
    final Iterator<KeyMessage<String,U>> transformed = Iterators.transform(stream.iterator(),
        new Function<MessageAndMetadata<String,U>, KeyMessage<String,U>>() {
          @Override
          public KeyMessage<String,U> apply(MessageAndMetadata<String,U> input) {
            return new KeyMessage<>(input.key(), input.message());
          }
        });

    modelManager = loadManagerInstance();
    new Thread(new LoggingRunnable() {
      @Override
      public void doRun() throws IOException {
        modelManager.consume(transformed);
      }
    }).start();

    dStream.foreachRDD(new SpeedLayerUpdate<>(modelManager, updateBroker, updateTopic));

    streamingContext.start();
  }

  public void await() {
    Preconditions.checkState(streamingContext != null);
    log.info("Waiting for streaming...");
    streamingContext.awaitTermination();
  }

  @Override
  public synchronized void close() {
    if (modelManager != null) {
      log.info("Shutting down model manager");
      modelManager.close();
      modelManager = null;
    }
    if (consumer != null) {
      log.info("Shutting down consumer");
      consumer.shutdown();
      consumer = null;
    }
    if (streamingContext != null) {
      log.info("Shutting down streaming context");
      streamingContext.stop(true, true);
      streamingContext = null;
    }
  }

  private JavaPairDStream<K,M> buildDStream() {
    Map<String,String> kafkaParams = new HashMap<>();
    kafkaParams.put("zookeeper.connect", inputQueueLockMaster);
    kafkaParams.put("group.id", "OryxGroup-SpeedLayer-" + System.currentTimeMillis());
    return KafkaUtils.createStream(
        streamingContext,
        keyClass,
        messageClass,
        keyDecoderClass,
        messageDecoderClass,
        kafkaParams,
        Collections.singletonMap(messageTopic, 1),
        StorageLevel.MEMORY_AND_DISK_2());
  }

  private SpeedModelManager<K,M,U> loadManagerInstance() {
    Class<?> managerClass = ClassUtils.loadClass(modelManagerClassName);

    if (SpeedModelManager.class.isAssignableFrom(managerClass)) {

      try {
        @SuppressWarnings("unchecked")
        SpeedModelManager<K,M,U> instance = ClassUtils.loadInstanceOf(
            modelManagerClassName,
            SpeedModelManager.class,
            new Class<?>[] { Config.class },
            new Object[] { config });
        return instance;

      } catch (IllegalArgumentException iae) {
        @SuppressWarnings("unchecked")
        SpeedModelManager<K,M,U> instance =
            ClassUtils.loadInstanceOf(modelManagerClassName, SpeedModelManager.class);
        return instance;
      }

    } else if (ScalaSpeedModelManager.class.isAssignableFrom(managerClass)) {

      try {
        @SuppressWarnings("unchecked")
        ScalaSpeedModelManager<K,M,U> instance = ClassUtils.loadInstanceOf(
            modelManagerClassName,
            ScalaSpeedModelManager.class,
            new Class<?>[] { Config.class },
            new Object[] { config });
        return new ScalaSpeedModelManagerAdapter<>(instance);

      } catch (IllegalArgumentException iae) {
        @SuppressWarnings("unchecked")
        ScalaSpeedModelManager<K,M,U> instance =
            ClassUtils.loadInstanceOf(modelManagerClassName, ScalaSpeedModelManager.class);
        return new ScalaSpeedModelManagerAdapter<>(instance);
      }

    } else {
      throw new IllegalArgumentException("Bad manager class: " + managerClass);
    }
  }

  private Decoder<U> loadDecoderInstance() {
    try {
      return ClassUtils.loadInstanceOf(updateDecoderClass);
    } catch (IllegalArgumentException iae) {
      // special case the Kafka decoder, which wants an optional nullable parameter unfortunately
      return ClassUtils.loadInstanceOf(updateDecoderClass.getName(),
                                       updateDecoderClass,
                                       new Class<?>[] { VerifiableProperties.class },
                                       new Object[] { null });
    }
  }

}
