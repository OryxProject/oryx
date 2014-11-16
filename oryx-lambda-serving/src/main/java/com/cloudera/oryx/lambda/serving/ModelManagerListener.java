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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.lang.ClassUtils;
import com.cloudera.oryx.common.lang.LoggingRunnable;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.lambda.KeyMessage;
import com.cloudera.oryx.lambda.QueueProducer;

@WebListener
public final class ModelManagerListener<K,M,U> implements ServletContextListener {

  private static final Logger log = LoggerFactory.getLogger(ModelManagerListener.class);

  public static final String MANAGER_KEY = ModelManagerListener.class.getName() + ".ModelManager";
  public static final String INPUT_PRODUCER_KEY =
      ModelManagerListener.class.getName() + ".InputProducer";

  private Config config;
  private String updateTopic;
  private String updateQueueLockMaster;
  private String inputTopic;
  private String inputQueueBroker;
  private String modelManagerClassName;
  private Class<? extends Decoder<U>> updateDecoderClass;
  private ConsumerConnector consumer;
  private ServingModelManager<U> modelManager;
  private QueueProducer<K,M> inputProducer;

  @SuppressWarnings("unchecked")
  public void init(ServletContext context) {
    String serializedConfig = context.getInitParameter(ConfigUtils.class.getName() + ".serialized");
    Preconditions.checkNotNull(serializedConfig);
    this.config = ConfigUtils.deserialize(serializedConfig);
    this.updateTopic = config.getString("oryx.update-queue.message.topic");
    this.updateQueueLockMaster = config.getString("oryx.update-queue.lock.master");
    this.inputTopic = config.getString("oryx.input-queue.message.topic");
    this.inputQueueBroker = config.getString("oryx.input-queue.broker");
    this.modelManagerClassName = config.getString("oryx.serving.model-manager-class");
    this.updateDecoderClass = (Class<? extends Decoder<U>>) ClassUtils.loadClass(
        config.getString("oryx.update-queue.message.decoder-class"), Decoder.class);
  }

  @Override
  public void contextInitialized(ServletContextEvent sce) {
    log.info("ModelManagerListener initializing");
    ServletContext context = sce.getServletContext();
    init(context);

    inputProducer = new QueueProducerImpl<>(inputQueueBroker, inputTopic);
    context.setAttribute(INPUT_PRODUCER_KEY, inputProducer);

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

    // Set the Model Manager in the Application scope
    context.setAttribute(MANAGER_KEY, modelManager);
  }

  @Override
  public void contextDestroyed(ServletContextEvent sce) {
    log.info("ModelManagerListener destroying");

    // Remove the Model Manager from Application scope
    sce.getServletContext().removeAttribute(MANAGER_KEY);

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
      consumer.shutdown();
      consumer = null;
    }
  }

  private ServingModelManager<U> loadManagerInstance() {
    Class<?> managerClass = ClassUtils.loadClass(modelManagerClassName);

    if (ServingModelManager.class.isAssignableFrom(managerClass)) {

      try {
        @SuppressWarnings("unchecked")
        ServingModelManager<U> instance = ClassUtils.loadInstanceOf(
            modelManagerClassName,
            ServingModelManager.class,
            new Class<?>[] { Config.class },
            new Object[] { config });
        return instance;

      } catch (IllegalArgumentException iae) {
        @SuppressWarnings("unchecked")
        ServingModelManager<U> instance =
            ClassUtils.loadInstanceOf(modelManagerClassName, ServingModelManager.class);
        return instance;
      }

    } else if (ScalaServingModelManager.class.isAssignableFrom(managerClass)) {

      try {
        @SuppressWarnings("unchecked")
        ScalaServingModelManager<U> instance = ClassUtils.loadInstanceOf(
            modelManagerClassName,
            ScalaServingModelManager.class,
            new Class<?>[] { Config.class },
            new Object[] { config });
        return new ScalaServingModelManagerAdapter<>(instance);

      } catch (IllegalArgumentException iae) {
        @SuppressWarnings("unchecked")
        ScalaServingModelManager<U> instance =
            ClassUtils.loadInstanceOf(modelManagerClassName, ScalaServingModelManager.class);
        return new ScalaServingModelManagerAdapter<>(instance);
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
