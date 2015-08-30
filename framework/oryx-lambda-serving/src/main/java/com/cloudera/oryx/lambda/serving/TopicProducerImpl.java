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

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import com.cloudera.oryx.api.TopicProducer;
import com.cloudera.oryx.common.settings.ConfigUtils;

/**
 * Wraps access to a Kafka message topic {@link Producer}.
 *
 * @param <K> key type to send
 * @param <M> message type to send
 */
public final class TopicProducerImpl<K,M> implements TopicProducer<K,M> {

  private final String updateBroker;
  private final String topic;
  private Producer<K,M> producer;

  public TopicProducerImpl(String updateBroker, String topic) {
    this.updateBroker = updateBroker;
    this.topic = topic;
  }

  @Override
  public String getUpdateBroker() {
    return updateBroker;
  }

  @Override
  public String getTopic() {
    return topic;
  }

  private synchronized Producer<K,M> getProducer() {
    // Lazy init
    if (producer == null) {
      producer = new Producer<>(new ProducerConfig(ConfigUtils.keyValueToProperties(
          "metadata.broker.list", updateBroker,
          "serializer.class", StringEncoder.class.getName(),
          "producer.type", "async",
          "queue.buffering.max.ms", 1000, // Make configurable?
          "batch.num.messages", 100,
          "compression.codec", "gzip",
          "compressed.topics", topic
      )));
    }
    return producer;
  }

  @Override
  public void send(K key, M message) {
    getProducer().send(new KeyedMessage<>(topic, key, message));
  }

  @Override
  public synchronized void close() {
    if (producer != null) {
      producer.close();
    }
  }

}
