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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.lang.ClassUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.kafka.util.KafkaUtils;

/**
 * Encapsulates commonality between Spark-based layer processes,
 * {@link com.cloudera.oryx.lambda.batch.BatchLayer} and
 * {@link com.cloudera.oryx.lambda.speed.SpeedLayer}
 *
 * @param <K> input topic key type
 * @param <M> input topic message type
 */
public abstract class AbstractSparkLayer<K,M> implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(AbstractSparkLayer.class);

  private final Config config;
  private final String id;
  private final String streamingMaster;
  private final String inputTopic;
  private final String inputTopicLockMaster;
  private final String inputBroker;
  private final String updateTopic;
  private final String updateTopicLockMaster;
  private final Class<K> keyClass;
  private final Class<M> messageClass;
  private final Class<? extends Decoder<K>> keyDecoderClass;
  private final Class<? extends Decoder<M>> messageDecoderClass;
  private final int generationIntervalSec;
  private final Map<String,Object> extraSparkConfig;

  @SuppressWarnings("unchecked")
  protected AbstractSparkLayer(Config config) {
    Objects.requireNonNull(config);
    log.info("Configuration:\n{}", ConfigUtils.prettyPrint(config));

    String group = getConfigGroup();
    this.config = config;
    String configuredID = ConfigUtils.getOptionalString(config, "oryx.id");
    this.id = configuredID == null ? UUID.randomUUID().toString() : configuredID;
    this.streamingMaster = config.getString("oryx." + group + ".streaming.master");
    this.inputTopic = config.getString("oryx.input-topic.message.topic");
    this.inputTopicLockMaster = config.getString("oryx.input-topic.lock.master");
    this.inputBroker = config.getString("oryx.input-topic.broker");
    this.updateTopic = ConfigUtils.getOptionalString(config, "oryx.update-topic.message.topic");
    this.updateTopicLockMaster = ConfigUtils.getOptionalString(config, "oryx.update-topic.lock.master");
    this.keyClass = ClassUtils.loadClass(config.getString("oryx.input-topic.message.key-class"));
    this.messageClass =
        ClassUtils.loadClass(config.getString("oryx.input-topic.message.message-class"));
    this.keyDecoderClass = (Class<? extends Decoder<K>>) ClassUtils.loadClass(
        config.getString("oryx.input-topic.message.key-decoder-class"), Decoder.class);
    this.messageDecoderClass = (Class<? extends Decoder<M>>) ClassUtils.loadClass(
        config.getString("oryx.input-topic.message.message-decoder-class"), Decoder.class);
    this.generationIntervalSec = config.getInt("oryx." + group + ".streaming.generation-interval-sec");

    this.extraSparkConfig = new HashMap<>();
    config.getConfig("oryx." + group + ".streaming.config").entrySet().forEach(e ->
      extraSparkConfig.put(e.getKey(), e.getValue().unwrapped())
    );

    Preconditions.checkArgument(generationIntervalSec > 0);
  }

  /**
   * @return layer-specific config grouping under "oryx", like "batch" or "speed"
   */
  protected abstract String getConfigGroup();

  /**
   * @return display name for layer like "BatchLayer"
   */
  protected abstract String getLayerName();

  protected final Config getConfig() {
    return config;
  }

  protected final String getID() {
    return id;
  }

  protected final String getGroupID() {
    return "OryxGroup-" + getLayerName() + "-" + getID();
  }

  protected final String getInputTopicLockMaster() {
    return inputTopicLockMaster;
  }

  protected final Class<K> getKeyClass() {
    return keyClass;
  }

  protected final Class<M> getMessageClass() {
    return messageClass;
  }

  protected final JavaStreamingContext buildStreamingContext() {
    log.info("Starting SparkContext with interval {} seconds", generationIntervalSec);

    SparkConf sparkConf = new SparkConf();

    // Only for tests, really
    if (sparkConf.getOption("spark.master").isEmpty()) {
      log.info("Overriding master to {} for tests", streamingMaster);
      sparkConf.setMaster(streamingMaster);
    }
    // Only for tests, really
    if (sparkConf.getOption("spark.app.name").isEmpty()) {
      String appName = "Oryx" + getLayerName();
      if (id != null) {
        appName = appName + "-" + id;
      }
      log.info("Overriding app name to {} for tests", appName);
      sparkConf.setAppName(appName);
    }
    extraSparkConfig.forEach((key, value) -> sparkConf.setIfMissing(key, value.toString()));

    // Turn this down to prevent long blocking at shutdown
    sparkConf.setIfMissing(
        "spark.streaming.gracefulStopTimeout",
        Long.toString(TimeUnit.MILLISECONDS.convert(generationIntervalSec, TimeUnit.SECONDS)));
    sparkConf.setIfMissing("spark.cleaner.ttl", Integer.toString(20 * generationIntervalSec));
    long generationIntervalMS =
        TimeUnit.MILLISECONDS.convert(generationIntervalSec, TimeUnit.SECONDS);

    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(sparkConf));
    return new JavaStreamingContext(jsc, new Duration(generationIntervalMS));
  }

  protected final JavaInputDStream<MessageAndMetadata<K,M>> buildInputDStream(
      JavaStreamingContext streamingContext) {

    Preconditions.checkArgument(
        KafkaUtils.topicExists(inputTopicLockMaster, inputTopic),
        "Topic %s does not exist; did you create it?", inputTopic);
    if (updateTopic != null && updateTopicLockMaster != null) {
      Preconditions.checkArgument(
          KafkaUtils.topicExists(updateTopicLockMaster, updateTopic),
          "Topic %s does not exist; did you create it?", updateTopic);
    }

    Map<String,String> kafkaParams = new HashMap<>();
    kafkaParams.put("zookeeper.connect", inputTopicLockMaster); // needed for SimpleConsumer later
    String groupID = getGroupID();
    kafkaParams.put("group.id", groupID);
    // Don't re-consume old messages from input by default
    kafkaParams.put("auto.offset.reset", "largest"); // becomes "latest" in Kafka 0.9+
    kafkaParams.put("metadata.broker.list", inputBroker);
    // Newer version of metadata.broker.list:
    kafkaParams.put("bootstrap.servers", inputBroker);

    Map<Pair<String,Integer>,Long> offsets =
        KafkaUtils.getOffsets(inputTopicLockMaster, groupID, inputTopic);
    KafkaUtils.fillInLatestOffsets(offsets, kafkaParams);
    log.info("Initial offsets: {}", offsets);

    // Ugly compiler-pleasing acrobatics:
    @SuppressWarnings("unchecked")
    Class<MessageAndMetadata<K,M>> streamClass =
        (Class<MessageAndMetadata<K,M>>) (Class<?>) MessageAndMetadata.class;

    Map<TopicAndPartition,Long> kafkaOffsets = new HashMap<>(offsets.size());
    offsets.forEach((tAndP, offset) -> kafkaOffsets.put(
        new TopicAndPartition(tAndP.getFirst(), tAndP.getSecond()), offset));

    return org.apache.spark.streaming.kafka.KafkaUtils.createDirectStream(
        streamingContext,
        keyClass,
        messageClass,
        keyDecoderClass,
        messageDecoderClass,
        streamClass,
        kafkaParams,
        kafkaOffsets,
        message -> message);
  }

}
