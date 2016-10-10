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

package com.cloudera.oryx.kafka.util;

import java.util.Collections;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.cloudera.oryx.api.KeyMessage;
import com.cloudera.oryx.common.collection.CloseableIterator;
import com.cloudera.oryx.common.settings.ConfigUtils;

/**
 * A iterator that consumes data from a Kafka topic.
 */
public final class ConsumeData implements Iterable<KeyMessage<String,String>> {

  private final String topic;
  private final int maxMessageSize;
  private final int kafkaPort;

  public ConsumeData(String topic, int kafkaPort) {
    this(topic, 1 << 16, kafkaPort);
  }

  ConsumeData(String topic, int maxMessageSize, int kafkaPort) {
    this.topic = topic;
    this.maxMessageSize = maxMessageSize;
    this.kafkaPort = kafkaPort;
  }

  @Override
  public CloseableIterator<KeyMessage<String,String>> iterator() {
    KafkaConsumer<String,String> consumer = new KafkaConsumer<>(
        ConfigUtils.keyValueToProperties(
          "group.id", "OryxGroup-ConsumeData",
          "bootstrap.servers", "localhost:" + kafkaPort,
          "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
          "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
          "max.partition.fetch.bytes", maxMessageSize,
          "auto.offset.reset", "earliest" // For tests, always start at the beginning
        ));
    consumer.subscribe(Collections.singletonList(topic));
    return new ConsumeDataIterator<>(consumer);
  }

}
