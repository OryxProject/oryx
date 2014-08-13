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

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;

import com.google.common.base.Function;
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

@WebListener
public final class ModelManagerListener<U> implements ServletContextListener {

  private static final Logger log = LoggerFactory.getLogger(ModelManagerListener.class);

  private final Config config;
  private final String updateTopic;
  private final String updateQueueLockMaster;
  private final Class<ServingModelManager<U>> modelManagerClass;
  private final Class<? extends Decoder<U>> updateDecoderClass;
  private ConsumerConnector consumer;
  private ServingModelManager<U> modelManager;

  @SuppressWarnings("unchecked")
  public ModelManagerListener() {
    this.config = ConfigUtils.getDefault();
    this.updateTopic = config.getString("update-queue.message.topic");
    this.updateQueueLockMaster = config.getString("update-queue.lock.master");
    this.modelManagerClass = (Class<ServingModelManager<U>>) ClassUtils.loadClass(
        config.getString("serving.model-manager-class"), ServingModelManager.class);
    this.updateDecoderClass = (Class<? extends Decoder<U>>) ClassUtils.loadClass(
        config.getString("update-queue.message.decoder-class"), Decoder.class);
  }

  @Override
  public void contextInitialized(ServletContextEvent sce) {

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
  }

  @Override
  public void contextDestroyed(ServletContextEvent sce) {
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
  }

  private ServingModelManager<U> loadManagerInstance() {
    try {
      return ClassUtils.loadInstanceOf(modelManagerClass.getName(),
                                       modelManagerClass,
                                       new Class<?>[] { Config.class },
                                       new Object[] { config });

    } catch (IllegalArgumentException iae) {
      log.info("{} lacks a constructor with Config arg, using no-arg constructor",
               modelManagerClass);
      return ClassUtils.loadInstanceOf(modelManagerClass);
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
