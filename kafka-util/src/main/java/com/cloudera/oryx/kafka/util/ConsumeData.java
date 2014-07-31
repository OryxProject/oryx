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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.common.collect.AbstractIterator;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A iterator that consumes data from a Kafka queue. When run on the command line, logs
 * the results to the console.
 */
public final class ConsumeData implements Iterable<String[]> {

  private static final Logger log = LoggerFactory.getLogger(ConsumeData.class);

  private final String topic;
  private final int zkPort;

  public ConsumeData() {
    this(ProduceData.DEFAULT_TOPIC);
  }

  public ConsumeData(String topic) {
    this(topic, LocalKafkaBroker.DEFAULT_ZK_PORT);
  }

  public ConsumeData(String topic, int zkPort) {
    this.topic = topic;
    this.zkPort = zkPort;
  }

  public static void main(String[] args) {
    int numThreads = args.length < 1 ? 1 : Integer.parseInt(args[0]);
    if (numThreads > 1) {
      for (int i = 0; i < numThreads; i++) {
        new Thread(new Runnable() {
          @Override
          public void run() {
            for (String[] km : new ConsumeData()) {
              log.info("{} = {}", km[0], km[1]);
            }
          }
        }).start();
      }
    } else {
      for (String[] km : new ConsumeData()) {
        log.info("{} = {}", km[0], km[1]);
      }
    }
  }

  @Override
  public Iterator<String[]> iterator() {
    Properties consumerProps = new Properties();
    consumerProps.setProperty("group.id", "OryxGroup-ConsumeData");
    consumerProps.setProperty("zookeeper.connect", "localhost:" + zkPort);
    ConsumerConfig consumerConfig = new ConsumerConfig(consumerProps);
    ConsumerConnector consumer = Consumer.createJavaConsumerConnector(consumerConfig);
    return new ConsumeDataIterator(topic, consumer);
  }

  private static final class ConsumeDataIterator extends AbstractIterator<String[]> {

    private final ConsumerConnector consumer;
    private final Iterator<MessageAndMetadata<String,String>> iterator;

    private ConsumeDataIterator(String topic, ConsumerConnector consumer) {
      this.consumer = consumer;
      Map<String,Integer> topicCountMap = new HashMap<>();
      topicCountMap.put(topic, 1);

      Map<String,List<KafkaStream<String,String>>> consumerMap =
          consumer.createMessageStreams(topicCountMap,
                                        new StringDecoder(null),
                                        new StringDecoder(null));
      KafkaStream<String,String> stream = consumerMap.get(topic).get(0);
      this.iterator = stream.iterator();
    }

    @Override
    protected String[] computeNext() {
      if (iterator.hasNext()) {
        MessageAndMetadata<String,String> mm = iterator.next();
        return new String[] { mm.key(), mm.message() };
      } else {
        consumer.shutdown();
        return endOfData();
      }
    }
  }

}
