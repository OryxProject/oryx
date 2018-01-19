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
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.api.KeyMessage;
import com.cloudera.oryx.api.TopicProducer;
import com.cloudera.oryx.api.serving.ScalaServingModelManager;
import com.cloudera.oryx.api.serving.ServingModelManager;
import com.cloudera.oryx.common.collection.CloseableIterator;
import com.cloudera.oryx.common.lang.ClassUtils;
import com.cloudera.oryx.common.lang.LoggingCallable;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.kafka.util.ConsumeDataIterator;
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
  private String updateTopicBroker;
  private boolean readOnly;
  private String inputTopic;
  private String inputTopicLockMaster;
  private String inputTopicBroker;
  private String modelManagerClassName;
  private Class<? extends Deserializer<U>> updateDecoderClass;
  private CloseableIterator<KeyMessage<String,U>> consumerIterator;
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
    this.updateTopicBroker = config.getString("oryx.update-topic.broker");
    this.readOnly = config.getBoolean("oryx.serving.api.read-only");
    if (!readOnly) {
      this.inputTopic = config.getString("oryx.input-topic.message.topic");
      this.inputTopicLockMaster = config.getString("oryx.input-topic.lock.master");
      this.inputTopicBroker = config.getString("oryx.input-topic.broker");
    }
    this.modelManagerClassName = config.getString("oryx.serving.model-manager-class");
    this.updateDecoderClass = (Class<? extends Deserializer<U>>) ClassUtils.loadClass(
        config.getString("oryx.update-topic.message.decoder-class"), Deserializer.class);
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

    KafkaConsumer<String,U> consumer = new KafkaConsumer<>(
        ConfigUtils.keyValueToProperties(
            "group.id", "OryxGroup-ServingLayer-" + UUID.randomUUID(),
            "bootstrap.servers", updateTopicBroker,
            "max.partition.fetch.bytes", maxMessageSize,
            "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer", updateDecoderClass.getName(),
            // Do start from the beginning of the update queue
            "auto.offset.reset", "earliest",
            // Be gentler on hosts that aren't connecting:
            "reconnect.backoff.ms", "1000",
            "reconnect.backoff.max.ms", "10000"
        ));
    consumer.subscribe(Collections.singletonList(updateTopic));
    consumerIterator = new ConsumeDataIterator<>(consumer);

    modelManager = loadManagerInstance();
    CountDownLatch threadStartLatch = new CountDownLatch(1);
    new Thread(LoggingCallable.log(() -> {
      try {
        // Can we do better than a default Hadoop config? Nothing else provides it here
        Configuration hadoopConf = new Configuration();
        threadStartLatch.countDown();
        modelManager.consume(consumerIterator, hadoopConf);
      } catch (Throwable t) {
        log.error("Error while consuming updates", t);
      } finally {
        // Ideally we would shut down ServingLayer, but not clear how to plumb that through
        // without assuming this has been run from ServingLayer and not a web app deployment
        close();
      }
    }).asRunnable(), "OryxServingLayerUpdateConsumerThread").start();

    try {
      threadStartLatch.await(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      log.warn("Failed to wait for OryxServingLayerUpdateConsumerThread; continue");
    }

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
    if (consumerIterator != null) {
      log.info("Shutting down consumer");
      consumerIterator.close();
      consumerIterator = null;
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

}
