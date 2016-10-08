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

import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;
import java.util.stream.StreamSupport;

import com.google.common.base.Preconditions;
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
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import com.cloudera.oryx.api.KeyMessage;
import com.cloudera.oryx.api.KeyMessageImpl;
import com.cloudera.oryx.api.speed.ScalaSpeedModelManager;
import com.cloudera.oryx.api.speed.SpeedModelManager;
import com.cloudera.oryx.common.lang.ClassUtils;
import com.cloudera.oryx.common.lang.LoggingCallable;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.lambda.AbstractSparkLayer;
import com.cloudera.oryx.lambda.UpdateOffsetsFn;

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
  private final int maxMessageSize;
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
    this.maxMessageSize = config.getInt("oryx.update-topic.message.max-size");
    this.updateTopicLockMaster = config.getString("oryx.update-topic.lock.master");
    this.modelManagerClassName = config.getString("oryx.speed.model-manager-class");
    this.updateDecoderClass = (Class<? extends Decoder<U>>) ClassUtils.loadClass(
        config.getString("oryx.update-topic.message.decoder-class"), Decoder.class);
    Preconditions.checkArgument(maxMessageSize > 0);
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
    JavaInputDStream<MessageAndMetadata<K,M>> kafkaDStream = buildInputDStream(streamingContext);
    JavaPairDStream<K,M> pairDStream =
        kafkaDStream.mapToPair(mAndM -> new Tuple2<>(mAndM.key(), mAndM.message()));

    consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(
        ConfigUtils.keyValueToProperties(
            "group.id", "OryxGroup-" + getLayerName() + "-" + UUID.randomUUID(),
            "zookeeper.connect", updateTopicLockMaster,
            "fetch.message.max.bytes", maxMessageSize,
            // Do start from the beginning of the update queue
            "auto.offset.reset", "smallest" // becomes "earliest" in Kafka 0.9+
            // Above are for Kafka 0.8; following are for 0.9+
            //"bootstrap.servers", updateTopicBroker,
            //"max.partition.fetch.bytes", maxMessageSize,
            //"key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
            //"value.deserializer", updateDecoderClass.getName()
        )));
    KafkaStream<String,U> stream =
        consumer.createMessageStreams(Collections.singletonMap(updateTopic, 1),
                                      new StringDecoder(null),
                                      loadDecoderInstance()).get(updateTopic).get(0);
    Iterator<KeyMessage<String,U>> transformed = StreamSupport.stream(stream.spliterator(), false)
        .map(input -> (KeyMessage<String,U>) new KeyMessageImpl<>(input.key(), input.message()))
        .iterator();

    modelManager = loadManagerInstance();
    Configuration hadoopConf = streamingContext.sparkContext().hadoopConfiguration();
    new Thread(LoggingCallable.log(() -> {
      try {
        modelManager.consume(transformed, hadoopConf);
      } catch (Throwable t) {
        log.error("Error while consuming updates", t);
        close();
      }
    }).asRunnable(), "OryxSpeedLayerUpdateConsumerThread").start();

    pairDStream.foreachRDD(new SpeedLayerUpdate<>(modelManager, updateBroker, updateTopic));

    // Must use the raw Kafka stream to get offsets
    kafkaDStream.foreachRDD(new UpdateOffsetsFn<>(getGroupID(), getInputTopicLockMaster()));

    log.info("Starting Spark Streaming");

    streamingContext.start();
  }

  public void await() throws InterruptedException {
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
      consumer.commitOffsets();
      consumer.shutdown();
      consumer = null;
    }
    if (streamingContext != null) {
      log.info("Shutting down Spark Streaming; this may take some time");
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
