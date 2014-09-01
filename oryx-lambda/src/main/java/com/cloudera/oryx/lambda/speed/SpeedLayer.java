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
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.typesafe.config.Config;
import kafka.consumer.Consumer$;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils$;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.lang.ClassUtils;
import com.cloudera.oryx.common.lang.LoggingRunnable;
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
  private final int generationIntervalSec;
  private final int blockIntervalSec;
  private final Class<? extends Decoder<U>> updateDecoderClass;
  private JavaStreamingContext streamingContext;
  private ConsumerConnector consumer;
  private SpeedModelManager<K,M,U> modelManager;

  @SuppressWarnings("unchecked")
  public SpeedLayer(Config config) {
    Preconditions.checkNotNull(config);
    this.config = config;
    this.streamingMaster = config.getString("speed.streaming.master");
    this.inputQueueLockMaster = config.getString("input-queue.lock.master");
    this.messageTopic = config.getString("input-queue.message.topic");
    this.updateBroker = config.getString("update-queue.broker");
    this.updateTopic = config.getString("update-queue.message.topic");
    this.updateQueueLockMaster = config.getString("update-queue.lock.master");
    this.modelManagerClassName = config.getString("speed.model-manager-class");
    this.generationIntervalSec = config.getInt("speed.generation-interval-sec");
    this.blockIntervalSec = config.getInt("speed.block-interval-sec");
    this.updateDecoderClass = (Class<? extends Decoder<U>>) ClassUtils.loadClass(
        config.getString("update-queue.message.decoder-class"), Decoder.class);
  }

  public synchronized void start() {
    log.info("Starting SparkContext for master {}, interval {} seconds",
             streamingMaster, generationIntervalSec);

    long blockIntervalMS = TimeUnit.MILLISECONDS.convert(blockIntervalSec, TimeUnit.SECONDS);

    SparkConf sparkConf = new SparkConf();
    sparkConf.setIfMissing("spark.streaming.blockInterval", Long.toString(blockIntervalMS));
    sparkConf.setMaster(streamingMaster);
    sparkConf.setAppName("OryxSpeedLayer");
    long batchDurationMS = TimeUnit.MILLISECONDS.convert(generationIntervalSec, TimeUnit.SECONDS);
    streamingContext = new JavaStreamingContext(sparkConf, new Duration(batchDurationMS));

    log.info("Creating message queue stream");

    JavaPairDStream<K,M> dStream = buildDStream();

    Properties consumerProps = new Properties();
    consumerProps.setProperty("group.id", "OryxGroup-SpeedLayer-" + System.currentTimeMillis());
    consumerProps.setProperty("zookeeper.connect", updateQueueLockMaster);
    ConsumerConfig consumerConfig = new ConsumerConfig(consumerProps);
    consumer = Consumer$.MODULE$.createJavaConsumerConnector(consumerConfig);
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
    // TODO for now we can only support default of Strings
    @SuppressWarnings("unchecked")
    JavaPairDStream<K,M> dStream = (JavaPairDStream<K,M>)
        KafkaUtils$.MODULE$.createStream(streamingContext,
                                         inputQueueLockMaster,
                                         // group should be unique
                                         "OryxGroup-SpeedLayer-" + System.currentTimeMillis(),
                                         Collections.singletonMap(messageTopic, 1));
    return dStream;
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
      log.warn("No no-arg constructor for {}; trying nullable one-arg", updateDecoderClass);
      // special case the Kafka decoder, which wants an optional nullable parameter unfortunately
      return ClassUtils.loadInstanceOf(updateDecoderClass.getName(),
                                       updateDecoderClass,
                                       new Class<?>[] { VerifiableProperties.class },
                                       new Object[] { null });
    }
  }

}
