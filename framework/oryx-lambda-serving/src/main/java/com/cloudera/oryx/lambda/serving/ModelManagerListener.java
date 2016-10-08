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

package com.cloudera.oryx.lambda.serving;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

import java.io.Closeable;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.StreamSupport;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.api.KeyMessage;
import com.cloudera.oryx.api.KeyMessageImpl;
import com.cloudera.oryx.api.TopicProducer;
import com.cloudera.oryx.api.serving.ScalaServingModelManager;
import com.cloudera.oryx.api.serving.ServingModelManager;
import com.cloudera.oryx.common.lang.ClassUtils;
import com.cloudera.oryx.common.lang.LoggingCallable;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.kafka.util.KafkaUtils;

/**
 * {@link ServletContextListener} that initializes a {@link ServingModelManager} at web
 * app startup time in the Serving Layer.
 *
 * @param <K> type of key written to input topic
 * @param <M> type of value written to input topic
 * @param <U> type of update/model read from update topic
 */
@WebListener
public final class ModelManagerListener<K,M,U> implements ServletContextListener, Closeable {

  private static final Logger log = LoggerFactory.getLogger(ModelManagerListener.class);

  static final String MANAGER_KEY = ModelManagerListener.class.getName() + ".ModelManager";
  private static final String INPUT_PRODUCER_KEY =
      ModelManagerListener.class.getName() + ".InputProducer";

  private Config config;
  private String updateTopic;
  private int maxMessageSize;
  private String updateTopicLockMaster;
  private boolean readOnly;
  private String inputTopic;
  private String inputTopicLockMaster;
  private String inputTopicBroker;
  private String modelManagerClassName;
  private Class<? extends Decoder<U>> updateDecoderClass;
  private ConsumerConnector consumer;
  private ServingModelManager<U> modelManager;
  private TopicProducer<K,M> inputProducer;

  @SuppressWarnings("unchecked")
  void init(ServletContext context) {
    String serializedConfig = context.getInitParameter(ConfigUtils.class.getName() + ".serialized");
    Objects.requireNonNull(serializedConfig);
    this.config = ConfigUtils.deserialize(serializedConfig);
    this.updateTopic = config.getString("oryx.update-topic.message.topic");
    this.maxMessageSize = config.getInt("oryx.update-topic.message.max-size");
    this.updateTopicLockMaster = config.getString("oryx.update-topic.lock.master");
    this.readOnly = config.getBoolean("oryx.serving.api.read-only");
    if (!readOnly) {
      this.inputTopic = config.getString("oryx.input-topic.message.topic");
      this.inputTopicLockMaster = config.getString("oryx.input-topic.lock.master");
      this.inputTopicBroker = config.getString("oryx.input-topic.broker");
    }
    this.modelManagerClassName = config.getString("oryx.serving.model-manager-class");
    this.updateDecoderClass = (Class<? extends Decoder<U>>) ClassUtils.loadClass(
        config.getString("oryx.update-topic.message.decoder-class"), Decoder.class);
    Preconditions.checkArgument(maxMessageSize > 0);
  }

  @Override
  public void contextInitialized(ServletContextEvent sce) {
    log.info("ModelManagerListener initializing");
    ServletContext context = sce.getServletContext();
    init(context);

    if (!readOnly) {
      Preconditions.checkArgument(KafkaUtils.topicExists(inputTopicLockMaster, inputTopic),
                                "Topic %s does not exist; did you create it?", inputTopic);
      Preconditions.checkArgument(KafkaUtils.topicExists(updateTopicLockMaster, updateTopic),
                                "Topic %s does not exist; did you create it?", updateTopic);
      inputProducer = new TopicProducerImpl<>(inputTopicBroker, inputTopic);
      context.setAttribute(INPUT_PRODUCER_KEY, inputProducer);
    }

    consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(
        ConfigUtils.keyValueToProperties(
            "group.id", "OryxGroup-ServingLayer-" + UUID.randomUUID(),
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
    new Thread(LoggingCallable.log(() -> {
      // Possible if it shuts down immediately; just exit
      if (modelManager == null) {
        return;
      }
      // Can we do better than a default Hadoop config? Nothing else provides it here
      Configuration hadoopConf = new Configuration();
      try {
        modelManager.consume(transformed, hadoopConf);
      } catch (Throwable t) {
        log.error("Error while consuming updates", t);
        // Ideally we would shut down ServingLayer, but not clear how to plumb that through
        // without assuming this has been run from ServingLayer and not a web app deployment
        close();
      }
    }).asRunnable(), "OryxServingLayerUpdateConsumerThread").start();

    // Set the Model Manager in the Application scope
    context.setAttribute(MANAGER_KEY, modelManager);
  }

  @Override
  public void contextDestroyed(ServletContextEvent sce) {
    log.info("ModelManagerListener destroying");
    // Slightly paranoid; remove objects from app scope manually
    ServletContext context = sce.getServletContext();
    for (Enumeration<String> names = context.getAttributeNames(); names.hasMoreElements();) {
      context.removeAttribute(names.nextElement());
    }

    close();

    // Hacky, but prevents Tomcat from complaining that ZK's cleanup thread 'leaked' since
    // it has a short sleep at its end
    try {
      Thread.sleep(1000);
    } catch (InterruptedException ie) {
      // continue
    }
  }

  @Override
  public synchronized void close() {
    log.info("ModelManagerListener closing");
    if (modelManager != null) {
      log.info("Shutting down model manager");
      modelManager.close();
      modelManager = null;
    }
    if (inputProducer != null) {
      log.info("Shutting down input producer");
      inputProducer.close();
      inputProducer = null;
    }
    if (consumer != null) {
      log.info("Shutting down consumer");
      consumer.commitOffsets();
      consumer.shutdown();
      consumer = null;
    }
  }

  @SuppressWarnings("unchecked")
  private ServingModelManager<U> loadManagerInstance() {
    Class<?> managerClass = ClassUtils.loadClass(modelManagerClassName);

    if (ServingModelManager.class.isAssignableFrom(managerClass)) {

      try {
        return ClassUtils.loadInstanceOf(
            modelManagerClassName,
            ServingModelManager.class,
            new Class<?>[] { Config.class },
            new Object[] { config });
      } catch (IllegalArgumentException iae) {
        return ClassUtils.loadInstanceOf(modelManagerClassName, ServingModelManager.class);
      }

    } else if (ScalaServingModelManager.class.isAssignableFrom(managerClass)) {

      try {
        return new ScalaServingModelManagerAdapter<>(ClassUtils.loadInstanceOf(
            modelManagerClassName,
            ScalaServingModelManager.class,
            new Class<?>[] { Config.class },
            new Object[] { config }));
      } catch (IllegalArgumentException iae) {
        return new ScalaServingModelManagerAdapter<>(ClassUtils.loadInstanceOf(
            modelManagerClassName, ScalaServingModelManager.class));
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
