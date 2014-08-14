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

import java.io.IOException;
import java.util.Properties;

import com.google.common.base.Preconditions;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.math3.random.RandomGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.random.RandomManager;

/**
 * A process that will continually send data to a Kafka topic. This is useful for testing
 * purposes. It will send strings, CSV-formatted random feature data, like "3,true,-0.135".
 */
public final class ProduceData {

  private static final Logger log = LoggerFactory.getLogger(ProduceData.class);

  private final RandomDatumGenerator<String,String> datumGenerator;
  private final int zkPort;
  private final int kafkaPort;
  private final String topic;
  private final int howMany;
  private final int intervalMsec;

  public ProduceData(RandomDatumGenerator<String,String> datumGenerator,
                     int zkPort,
                     int kafkaPort,
                     String topic,
                     int howMany,
                     int intervalMsec) {
    Preconditions.checkNotNull(datumGenerator);
    Preconditions.checkArgument(zkPort > 0);
    Preconditions.checkArgument(kafkaPort > 0);
    Preconditions.checkNotNull(topic);
    Preconditions.checkArgument(howMany > 0);
    Preconditions.checkArgument(intervalMsec > 0);
    this.datumGenerator = datumGenerator;
    this.zkPort = zkPort;
    this.kafkaPort = kafkaPort;
    this.topic = topic;
    this.howMany = howMany;
    this.intervalMsec = intervalMsec;
  }

  public static void main(String[] args) throws Exception {
    int howMany = Integer.parseInt(args[0]);
    int intervalMsec = Integer.parseInt(args[1]);
    String topic = args[2];
    int zkPort = Integer.parseInt(args[3]);
    int kafkaPort = Integer.parseInt(args[4]);

    ProduceData producer = new ProduceData(new DefaultCSVDatumGenerator(),
                                           zkPort,
                                           kafkaPort,
                                           topic,
                                           howMany,
                                           intervalMsec);
    producer.start();
  }

  public void start() throws InterruptedException, IOException {
    KafkaUtils.maybeCreateTopic("localhost", zkPort, topic);
    RandomGenerator random = RandomManager.getRandom();

    Properties producerProps = new Properties();
    producerProps.setProperty("metadata.broker.list", "localhost:" + kafkaPort);
    producerProps.setProperty("serializer.class", "kafka.serializer.StringEncoder");

    Thread.sleep(intervalMsec);
    Producer<String,String> producer = new Producer<>(new ProducerConfig(producerProps));
    try {
      for (int i = 0; i < howMany; i++) {
        Pair<String,String> datum = datumGenerator.generate(i, random);
        KeyedMessage<String,String> keyedMessage =
            new KeyedMessage<>(topic, datum.getFirst(), datum.getSecond());
        producer.send(keyedMessage);
        log.debug("Sent datum {} = {}", keyedMessage.key(), keyedMessage.message());
        Thread.sleep(intervalMsec);
      }
    } finally {
      producer.close();
    }
  }

  public void deleteTopic() {
    KafkaUtils.deleteTopic("localhost", zkPort, topic);
  }

}
