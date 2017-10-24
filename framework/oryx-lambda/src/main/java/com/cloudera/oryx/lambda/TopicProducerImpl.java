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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

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
  private final boolean async;
  private Producer<K,M> producer;

  public TopicProducerImpl(String updateBroker, String topic, boolean async) {
    this.updateBroker = updateBroker;
    this.topic = topic;
    this.async = async;
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
    // Lazy init; also handles case where object has been serialized and Producer
    // needs to be recreated
    if (producer == null) {
      producer = new KafkaProducer<>(ConfigUtils.keyValueToProperties(
          "bootstrap.servers", updateBroker,
          "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
          "value.serializer", "org.apache.kafka.common.serialization.StringSerializer",
          "linger.ms", async ? 1000 : 0, // Make configurable?
          "batch.size", async ? 1 << 14 : 0,
          "compression.type", "gzip",
          "acks", 1,
          "max.request.size", 1 << 26 // TODO
      ));
    }
    return producer;
  }

  @Override
  public void send(K key, M message) {
    getProducer().send(new ProducerRecord<>(topic, key, message));
  }

  @Override
  public synchronized void close() {
    if (producer != null) {
      producer.close();
    }
  }

}
