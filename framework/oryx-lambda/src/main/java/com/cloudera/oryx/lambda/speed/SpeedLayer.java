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

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;

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
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.api.KeyMessage;
import com.cloudera.oryx.api.KeyMessageImpl;
import com.cloudera.oryx.api.speed.ScalaSpeedModelManager;
import com.cloudera.oryx.api.speed.SpeedModelManager;
import com.cloudera.oryx.common.lang.ClassUtils;
import com.cloudera.oryx.common.lang.LoggingRunnable;
import com.cloudera.oryx.lambda.AbstractSparkLayer;

/**
 * Main entry point for Oryx Speed Layer.
 *
 * @param <K> type of key read from input topic
 * @param <M> type of message read from input topic
 * @param <U> type of update message read/written
 */
public final class SpeedLayer<K,M,U> extends AbstractSparkLayer<K,M> {

  private static final Logger log = LoggerFactory.getLogger(SpeedLayer.class);

  private final String updateBroker;
  private final String updateTopic;
  private final String updateTopicLockMaster;
  private final String modelManagerClassName;
  private final Class<? extends Decoder<U>> updateDecoderClass;
  private JavaStreamingContext streamingContext;
  private ConsumerConnector consumer;
  private SpeedModelManager<K,M,U> modelManager;

  @SuppressWarnings("unchecked")
  public SpeedLayer(Config config) {
    super(config);
    this.updateBroker = config.getString("oryx.update-topic.broker");
    this.updateTopic = config.getString("oryx.update-topic.message.topic");
    this.updateTopicLockMaster = config.getString("oryx.update-topic.lock.master");
    this.modelManagerClassName = config.getString("oryx.speed.model-manager-class");
    this.updateDecoderClass = (Class<? extends Decoder<U>>) ClassUtils.loadClass(
        config.getString("oryx.update-topic.message.decoder-class"), Decoder.class);
  }

  @Override
  protected String getConfigGroup() {
    return "speed";
  }

  @Override
  protected String getLayerName() {
    return "SpeedLayer";
  }

  public synchronized void start() {
    String id = getID();
    if (id != null) {
      log.info("Starting Speed Layer {}", id);
    }

    streamingContext = buildStreamingContext();
    log.info("Creating message stream from topic");
    JavaPairDStream<K,M> dStream = buildInputDStream(streamingContext);

    Properties consumerProps = new Properties();
    consumerProps.setProperty("group.id",
                              "OryxGroup-" + getLayerName() + "-" + System.currentTimeMillis());
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
            return new KeyMessageImpl<>(input.key(), input.message());
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

  @SuppressWarnings("unchecked")
  private SpeedModelManager<K,M,U> loadManagerInstance() {
    Class<?> managerClass = ClassUtils.loadClass(modelManagerClassName);

    if (SpeedModelManager.class.isAssignableFrom(managerClass)) {

      try {
        return ClassUtils.loadInstanceOf(
            modelManagerClassName,
            SpeedModelManager.class,
            new Class<?>[] { Config.class },
            new Object[] { getConfig() });
      } catch (IllegalArgumentException iae) {
        return ClassUtils.loadInstanceOf(modelManagerClassName, SpeedModelManager.class);
      }

    } else if (ScalaSpeedModelManager.class.isAssignableFrom(managerClass)) {

      try {
        return new ScalaSpeedModelManagerAdapter<>(ClassUtils.loadInstanceOf(
            modelManagerClassName,
            ScalaSpeedModelManager.class,
            new Class<?>[] { Config.class },
            new Object[] { getConfig() }));
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
