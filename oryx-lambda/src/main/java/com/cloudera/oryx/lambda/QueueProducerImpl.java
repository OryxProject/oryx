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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

/**
 * Wraps access to a Kafka message queue {@link Producer}, including logic to instantiate the
 * object. This is a wrapper that can be serialized and re-create the {@link Producer}
 * remotely.
 */
public final class QueueProducerImpl<K,M> implements QueueProducer<K,M> {

  private final String updateBroker;
  private final String topic;
  private transient Producer<K,M> producer;

  public QueueProducerImpl(String updateBroker, String topic) {
    this.updateBroker = updateBroker;
    this.topic = topic;
    initProducer();
  }

  private void initProducer() {
    Properties producerProps = new Properties();
    producerProps.setProperty("metadata.broker.list", updateBroker);
    producerProps.setProperty("serializer.class", StringEncoder.class.getName());
    producer = new Producer<>(new ProducerConfig(producerProps));
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    initProducer();
  }

  @Override
  public void send(K key, M message) {
    producer.send(new KeyedMessage<>(topic, key, message));
  }

  @Override
  public void send(M message) {
    producer.send(new KeyedMessage<K, M>(topic, message));
  }

  @Override
  public void close() {
    if (producer != null) {
      producer.close();
    }
  }

}
