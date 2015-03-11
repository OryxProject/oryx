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

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import com.cloudera.oryx.api.TopicProducer;

/**
 * Wraps access to a Kafka message topic {@link Producer}.
 */
public final class TopicProducerImpl<K,M> implements TopicProducer<K,M> {

  private final String topic;
  private final Producer<K,M> producer;

  public TopicProducerImpl(String updateBroker, String topic) {
    this.topic = topic;
    Properties producerProps = new Properties();
    producerProps.setProperty("metadata.broker.list", updateBroker);
    producerProps.setProperty("serializer.class", StringEncoder.class.getName());
    producer = new Producer<>(new ProducerConfig(producerProps));
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
