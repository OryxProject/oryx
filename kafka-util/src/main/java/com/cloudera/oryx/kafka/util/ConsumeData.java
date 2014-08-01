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

import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.collection.CloseableIterator;

/**
 * A iterator that consumes data from a Kafka queue. When run on the command line, logs
 * the results to the console.
 */
public final class ConsumeData implements Iterable<String[]> {

  private static final Logger log = LoggerFactory.getLogger(ConsumeData.class);

  private final String topic;
  private final int zkPort;

  public ConsumeData(String topic, int zkPort) {
    this.topic = topic;
    this.zkPort = zkPort;
  }

  public static void main(String[] args) {
    String topic = args[0];
    int zkPort = Integer.parseInt(args[1]);
    for (String[] km : new ConsumeData(topic, zkPort)) {
      log.info("{} = {}", km[0], km[1]);
    }
  }

  @Override
  public CloseableIterator<String[]> iterator() {
    Properties consumerProps = new Properties();
    consumerProps.setProperty("group.id", "OryxGroup-ConsumeData");
    consumerProps.setProperty("zookeeper.connect", "localhost:" + zkPort);
    ConsumerConfig consumerConfig = new ConsumerConfig(consumerProps);
    ConsumerConnector consumer = Consumer.createJavaConsumerConnector(consumerConfig);
    return new ConsumeDataIterator(topic, consumer);
  }

}
