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
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import com.cloudera.oryx.common.lang.ClassUtils;
import com.cloudera.oryx.common.random.RandomManager;
import com.cloudera.oryx.common.settings.ConfigUtils;

/**
 * Encapsulates commonality between Spark-based layer processes,
 * {@link com.cloudera.oryx.lambda.batch.BatchLayer} and
 * {@link com.cloudera.oryx.lambda.speed.SpeedLayer}
 */
public abstract class AbstractSparkLayer<K,M> implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(AbstractSparkLayer.class);

  private final Config config;
  private final String id;
  private final String streamingMaster;
  private final String inputTopic;
  private final String inputTopicLockMaster;
  private final String inputBroker;
  private final Class<K> keyClass;
  private final Class<M> messageClass;
  private final Class<? extends Decoder<K>> keyDecoderClass;
  private final Class<? extends Decoder<M>> messageDecoderClass;
  private final int numExecutors;
  private final int executorCores;
  private final String executorMemoryString;
  //private final String driverMemoryString;
  private final int generationIntervalSec;
  private final int blockIntervalSec;
  private final int uiPort;

  @SuppressWarnings("unchecked")
  protected AbstractSparkLayer(Config config) {
    Preconditions.checkNotNull(config);
    log.info("Configuration:\n{}", ConfigUtils.prettyPrint(config));

    String group = getConfigGroup();
    this.config = config;
    String configuredID = ConfigUtils.getOptionalString(config, "oryx.id");
    this.id = configuredID == null ? generateRandomID() : configuredID;
    this.streamingMaster = config.getString("oryx." + group + ".streaming.master");
    this.inputTopic = config.getString("oryx.input-topic.message.topic");
    this.inputTopicLockMaster = config.getString("oryx.input-topic.lock.master");
    this.inputBroker = config.getString("oryx.input-topic.broker");
    this.keyClass = ClassUtils.loadClass(config.getString("oryx.input-topic.message.key-class"));
    this.messageClass =
        ClassUtils.loadClass(config.getString("oryx.input-topic.message.message-class"));
    this.keyDecoderClass = (Class<? extends Decoder<K>>) ClassUtils.loadClass(
        config.getString("oryx.input-topic.message.key-decoder-class"), Decoder.class);
    this.messageDecoderClass = (Class<? extends Decoder<M>>) ClassUtils.loadClass(
        config.getString("oryx.input-topic.message.message-decoder-class"), Decoder.class);
    this.numExecutors = config.getInt("oryx." + group + ".streaming.num-executors");
    this.executorCores = config.getInt("oryx." + group + ".streaming.executor-cores");
    this.executorMemoryString = config.getString("oryx." + group + ".streaming.executor-memory");
    //this.driverMemoryString = config.getString("oryx." + group + ".streaming.driver-memory");
    this.generationIntervalSec =
        config.getInt("oryx." + group + ".streaming.generation-interval-sec");
    this.blockIntervalSec = config.getInt("oryx." + group + ".streaming.block-interval-sec");
    this.uiPort = config.getInt("oryx." + group + ".ui.port");

    Preconditions.checkArgument(numExecutors >= 1);
    Preconditions.checkArgument(executorCores >= 1);
    Preconditions.checkArgument(generationIntervalSec > 0);
    Preconditions.checkArgument(blockIntervalSec > 0);
    Preconditions.checkArgument(uiPort > 0);
  }

  private static String generateRandomID() {
    return Integer.toString(RandomManager.getRandom().nextInt() & 0x7FFFFFFF);
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
    log.info("Starting SparkContext for master {}, interval {} seconds",
             streamingMaster, generationIntervalSec);

    SparkConf sparkConf = new SparkConf();

    sparkConf.setMaster(streamingMaster);
    sparkConf.setAppName("Oryx" + getLayerName());

    sparkConf.setIfMissing("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.setIfMissing("spark.io.compression.codec", "lzf");
    sparkConf.setIfMissing("spark.speculation", "true");
    sparkConf.setIfMissing("spark.shuffle.manager", "sort");

    // Enable dynamic allocation for YARN
    if (streamingMaster.startsWith("yarn")) { // yarn-client, yarn-cluster
      sparkConf.setIfMissing("spark.shuffle.service.enabled", "true");
      sparkConf.setIfMissing("spark.dynamicAllocation.enabled", "true");
      sparkConf.setIfMissing("spark.dynamicAllocation.maxExecutors",
                             Integer.toString(numExecutors));
      sparkConf.setIfMissing("spark.dynamicAllocation.executorIdleTimeout", "60");
    } else {
      sparkConf.setIfMissing("spark.executor.instances", Integer.toString(numExecutors));
    }

    sparkConf.setIfMissing("spark.executor.cores", Integer.toString(executorCores));
    sparkConf.setIfMissing("spark.executor.memory", executorMemoryString);
    //sparkConf.setIfMissing("spark.driver.memory", driverMemoryString);

    sparkConf.setIfMissing(
        "spark.streaming.blockInterval",
        Long.toString(TimeUnit.MILLISECONDS.convert(blockIntervalSec, TimeUnit.SECONDS)));
    // Turn this down to prevent long blocking at shutdown
    sparkConf.setIfMissing(
        "spark.streaming.gracefulStopTimeout",
        Long.toString(TimeUnit.MILLISECONDS.convert(generationIntervalSec, TimeUnit.SECONDS)));
    sparkConf.setIfMissing("spark.cleaner.ttl", Integer.toString(20 * generationIntervalSec));
    sparkConf.setIfMissing("spark.logConf", "true");
    sparkConf.setIfMissing("spark.ui.port", Integer.toString(uiPort));
    sparkConf.setIfMissing("spark.ui.showConsoleProgress", "false");

    sparkConf.setIfMissing("spark.driver.userClassPathFirst", "true");
    sparkConf.setIfMissing("spark.executor.userClassPathFirst", "true");

    long generationIntervalMS =
        TimeUnit.MILLISECONDS.convert(generationIntervalSec, TimeUnit.SECONDS);

    return new JavaStreamingContext(new JavaSparkContext(sparkConf),
                                    new Duration(generationIntervalMS));
  }

  protected final JavaInputDStream<MessageAndMetadata<K,M>> buildInputDStream(
      JavaStreamingContext streamingContext) {
    Map<String,String> kafkaParams = new HashMap<>();
    //kafkaParams.put("zookeeper.connect", inputTopicLockMaster);
    String groupID = getGroupID();
    kafkaParams.put("group.id", groupID);
    // Don't re-consume old messages from input by default
    kafkaParams.put("auto.offset.reset", "largest");
    kafkaParams.put("metadata.broker.list", inputBroker);
    // Newer version of metadata.broker.list:
    kafkaParams.put("bootstrap.servers", inputBroker);

    Map<TopicAndPartition,Long> offsets =
        com.cloudera.oryx.kafka.util.KafkaUtils.getOffsets(inputTopicLockMaster,
                                                           getGroupID(),
                                                           inputTopic);
    log.info("Initial offsets: {}", offsets);

    // Ugly compiler-pleasing acrobatics:
    @SuppressWarnings("unchecked")
    Class<MessageAndMetadata<K,M>> streamClass =
        (Class<MessageAndMetadata<K,M>>) (Class<?>) MessageAndMetadata.class;
    Function<MessageAndMetadata<K,M>,MessageAndMetadata<K,M>> identity = identity();

    return KafkaUtils.createDirectStream(streamingContext,
                                         keyClass,
                                         messageClass,
                                         keyDecoderClass,
                                         messageDecoderClass,
                                         streamClass,
                                         kafkaParams,
                                         offsets,
                                         identity);
  }

  private static <T> Function<T,T> identity() {
    return new Function<T,T>() {
      @Override
      public T call(T t) {
        return t;
      }
    };
  }

  protected static <T,U> PairFunction<MessageAndMetadata<T,U>,T,U> kmToPair() {
    return new PairFunction<MessageAndMetadata<T, U>, T, U>() {
      @Override
      public Tuple2<T, U> call(MessageAndMetadata<T, U> km) {
        return new Tuple2<>(km.key(), km.message());
      }
    };
  }

}
