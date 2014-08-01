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

import com.google.common.base.Preconditions;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.math3.random.RandomGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.random.RandomManager;

/**
 * A process that will continually send data to a Kafka topic. This is useful for testing
 * purposes. It will send strings, CSV-formatted random feature data, like "3,true,-0.135".
 */
public final class ProduceData {

  private static final Logger log = LoggerFactory.getLogger(ProduceData.class);

  public static final String DEFAULT_TOPIC = "OryxInput";
  public static final int DEFAULT_HOW_MANY = 100;
  public static final int DEFAULT_INTERVAL_MSEC = 100;

  private final RandomDatumGenerator<String> datumGenerator;
  private final int zkPort;
  private final int kafkaPort;
  private final String topic;
  private final int howMany;
  private final int intervalMsec;

  public ProduceData() {
    this(new DefaultCSVDatumGenerator(), DEFAULT_HOW_MANY, DEFAULT_INTERVAL_MSEC);
  }

  public ProduceData(int zkPort, int kafkaPort, String topic) {
    this(new DefaultCSVDatumGenerator(),
         zkPort, kafkaPort,
         topic,
         DEFAULT_HOW_MANY, DEFAULT_INTERVAL_MSEC);
  }

  public ProduceData(RandomDatumGenerator<String> datumGenerator, int howMany, int intervalMsec) {
    this(datumGenerator,
         LocalKafkaBroker.DEFAULT_ZK_PORT,
         LocalKafkaBroker.DEFAULT_PORT,
         DEFAULT_TOPIC,
         howMany,
         intervalMsec);
  }

  public ProduceData(RandomDatumGenerator<String> datumGenerator,
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
    ProduceData producer = new ProduceData();
    producer.start();
  }

  public void start() throws InterruptedException {
    KafkaUtils.maybeCreateTopic("localhost", zkPort, topic);
    RandomGenerator random = RandomManager.getRandom();

    Properties producerProps = new Properties();
    producerProps.setProperty("metadata.broker.list", "localhost:" + kafkaPort);
    producerProps.setProperty("serializer.class", "kafka.serializer.StringEncoder");

    Thread.sleep(intervalMsec);
    Producer<String,String> producer = new Producer<>(new ProducerConfig(producerProps));
    try {
      for (int i = 0; i < howMany; i++) {
        String datum = datumGenerator.generate(i, random);
        KeyedMessage<String,String> keyedMessage =
            new KeyedMessage<>(topic, Integer.toString(i), datum);
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
