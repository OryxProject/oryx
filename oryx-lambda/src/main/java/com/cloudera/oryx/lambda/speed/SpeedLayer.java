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
import com.cloudera.oryx.common.lang.LoggingRunnable;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.lambda.KeyMessage;

/**
 * Main entry point for Oryx Speed Layer.
 *
 * @param <K> type of key read from input topic
 * @param <M> type of message read from input topic
 * @param <U> type of update message read/written
 */
public final class SpeedLayer<K,M,U> implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(SpeedLayer.class);

  private final Config config;
  private final String streamingMaster;
  private final String inputTopicLockMaster;
  private final String messageTopic;
  private final String updateBroker;
  private final String updateTopic;
  private final String updateTopicLockMaster;
  private final String modelManagerClassName;
  private final int numExecutors;
  private final int executorCores;
  private final String executorMemoryString;
  private final String driverMemoryString;
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
  private final int uiPort;

  @SuppressWarnings("unchecked")
  public SpeedLayer(Config config) {
    Preconditions.checkNotNull(config);
    log.info("Configuration:\n{}", ConfigUtils.prettyPrint(config));
    this.config = config;
    this.streamingMaster = config.getString("oryx.speed.streaming.master");
    this.inputTopicLockMaster = config.getString("oryx.input-topic.lock.master");
    this.messageTopic = config.getString("oryx.input-topic.message.topic");
    this.updateBroker = config.getString("oryx.update-topic.broker");
    this.updateTopic = config.getString("oryx.update-topic.message.topic");
    this.updateTopicLockMaster = config.getString("oryx.update-topic.lock.master");
    this.modelManagerClassName = config.getString("oryx.speed.model-manager-class");
    this.numExecutors = config.getInt("oryx.speed.streaming.num-executors");
    this.executorCores = config.getInt("oryx.speed.streaming.executor-cores");
    this.executorMemoryString = config.getString("oryx.speed.streaming.executor-memory");
    this.driverMemoryString = config.getString("oryx.speed.streaming.driver-memory");
    this.generationIntervalSec = config.getInt("oryx.speed.streaming.generation-interval-sec");
    this.blockIntervalSec = config.getInt("oryx.speed.streaming.block-interval-sec");
    this.keyDecoderClass = (Class<? extends Decoder<?>>) ClassUtils.loadClass(
        config.getString("oryx.input-topic.message.key-decoder-class"), Decoder.class);
    this.messageDecoderClass = (Class<? extends Decoder<?>>) ClassUtils.loadClass(
        config.getString("oryx.input-topic.message.message-decoder-class"), Decoder.class);
    this.updateDecoderClass = (Class<? extends Decoder<U>>) ClassUtils.loadClass(
        config.getString("oryx.update-topic.message.decoder-class"), Decoder.class);
    this.keyClass = ClassUtils.loadClass(config.getString("oryx.input-topic.message.key-class"));
    this.messageClass =
        ClassUtils.loadClass(config.getString("oryx.input-topic.message.message-class"));
    this.uiPort = config.getInt("oryx.speed.ui.port");

    Preconditions.checkArgument(numExecutors >= 1);
    Preconditions.checkArgument(executorCores >= 1);
    Preconditions.checkArgument(generationIntervalSec > 0);
    Preconditions.checkArgument(blockIntervalSec > 0);
    Preconditions.checkArgument(uiPort > 0);
  }

  public synchronized void start() {
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

    sparkConf.setMaster(streamingMaster);
    sparkConf.setAppName("OryxSpeedLayer");
    long batchDurationMS = TimeUnit.MILLISECONDS.convert(generationIntervalSec, TimeUnit.SECONDS);
    JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

    streamingContext = new JavaStreamingContext(sparkContext, new Duration(batchDurationMS));

    log.info("Creating message stream from topic");

    JavaPairDStream<K,M> dStream = buildDStream();

    Properties consumerProps = new Properties();
    consumerProps.setProperty("group.id", "OryxGroup-SpeedLayer-" + System.currentTimeMillis());
    consumerProps.setProperty("zookeeper.connect", updateTopicLockMaster);
    // Do start from the beginning of the update queue
    consumerProps.setProperty("auto.offset.reset", "smallest");
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
    kafkaParams.put("zookeeper.connect", inputTopicLockMaster);
    kafkaParams.put("group.id", "OryxGroup-SpeedLayer-" + System.currentTimeMillis());
    // Don't re-consume old messages from input
    kafkaParams.put("auto.offset.reset", "largest");
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

  @SuppressWarnings("unchecked")
  private SpeedModelManager<K,M,U> loadManagerInstance() {
    Class<?> managerClass = ClassUtils.loadClass(modelManagerClassName);

    if (SpeedModelManager.class.isAssignableFrom(managerClass)) {

      try {
        return ClassUtils.loadInstanceOf(
            modelManagerClassName,
            SpeedModelManager.class,
            new Class<?>[] { Config.class },
            new Object[] { config });
      } catch (IllegalArgumentException iae) {
        return ClassUtils.loadInstanceOf(modelManagerClassName, SpeedModelManager.class);
      }

    } else if (ScalaSpeedModelManager.class.isAssignableFrom(managerClass)) {

      try {
        return new ScalaSpeedModelManagerAdapter<>(ClassUtils.loadInstanceOf(
            modelManagerClassName,
            ScalaSpeedModelManager.class,
            new Class<?>[] { Config.class },
            new Object[] { config }));
      } catch (IllegalArgumentException iae) {
        return new ScalaSpeedModelManagerAdapter<>(ClassUtils.loadInstanceOf(
            modelManagerClassName, ScalaSpeedModelManager.class));
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
