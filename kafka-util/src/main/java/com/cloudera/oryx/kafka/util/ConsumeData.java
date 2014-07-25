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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A process that will consume data from a Kafka queue, optionally in parallel, and log
 * the results to the console.
 */
public final class ConsumeData {

  private static final Logger log = LoggerFactory.getLogger(ConsumeData.class);

  public static final String DEFAULT_GROUP = "OryxGroup";

  private final int zkPort;
  private final String topic;
  private final String groupID;

  public ConsumeData() {
    this(DEFAULT_GROUP);
  }

  public ConsumeData(String groupID) {
    this(LocalKafkaBroker.DEFAULT_ZK_PORT, ProduceData.DEFAULT_TOPIC, groupID);
  }

  public ConsumeData(int zkPort, String topic, String groupID) {
    this.zkPort = zkPort;
    this.topic = topic;
    this.groupID = groupID;
  }

  public static void main(String[] args) {
    int numThreads = args.length < 1 ? 1 : Integer.parseInt(args[0]);
    if (numThreads > 1) {
      for (int i = 0; i < numThreads; i++) {
        final String group = DEFAULT_GROUP + i;
        new Thread(new Runnable() {
          @Override
          public void run() {
            new ConsumeData(group).start();
          }
        }).start();
      }
    } else {
      new ConsumeData(DEFAULT_GROUP).start();
    }
  }

  public void start() {
    Properties consumerProps = new Properties();
    consumerProps.setProperty("group.id", groupID);
    consumerProps.setProperty("zookeeper.connect", "localhost:" + zkPort);

    ConsumerConfig consumerConfig = new ConsumerConfig(consumerProps);
    ConsumerConnector consumer = Consumer.createJavaConsumerConnector(consumerConfig);

    Map<String,Integer> topicCountMap = new HashMap<>();
    topicCountMap.put(topic, 1);

    Map<String,List<KafkaStream<String,String>>> consumerMap =
        consumer.createMessageStreams(topicCountMap, new StringDecoder(null), new StringDecoder(null));
    List<KafkaStream<String,String>> streams = consumerMap.get(topic);

    try {
      for (MessageAndMetadata<String, String> msg : streams.get(0)) {
        log.info("{} = {}", msg.key(), msg.message());
      }
    } finally {
      consumer.shutdown();
    }
  }

}
