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

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;

import com.cloudera.oryx.api.KeyMessage;
import com.cloudera.oryx.common.collection.CloseableIterator;
import com.cloudera.oryx.common.settings.ConfigUtils;

/**
 * A iterator that consumes data from a Kafka topic.
 */
public final class ConsumeData implements Iterable<KeyMessage<String,String>> {

  private final String topic;
  private final int maxMessageSize;
  private final int zkPort;

  public ConsumeData(String topic, int zkPort) {
    this(topic, 1 << 16, zkPort);
  }

  public ConsumeData(String topic, int maxMessageSize, int zkPort) {
    this.topic = topic;
    this.maxMessageSize = maxMessageSize;
    this.zkPort = zkPort;
  }

  @Override
  public CloseableIterator<KeyMessage<String,String>> iterator() {
    ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(
        ConfigUtils.keyValueToProperties(
          "group.id", "OryxGroup-ConsumeData",
          "zookeeper.connect", "localhost:" + zkPort,
          "fetch.message.max.bytes", maxMessageSize,
          "auto.offset.reset", "smallest" // For tests, always start at the beginning; "earliest"
          // Above are for Kafka 0.8; following are for 0.9+
          //"bootstrap.servers", "localhost:" + kafkaPort,
          //"key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
          //"value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
          //"max.partition.fetch.bytes", maxMessageSize
        )));
    return new ConsumeDataIterator(topic, consumer);
  }

}
