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

import java.util.Objects;

import com.google.common.base.Preconditions;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.math3.random.RandomGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.random.RandomManager;
import com.cloudera.oryx.common.settings.ConfigUtils;

/**
 * A process that will continually send data to a Kafka topic. This is useful for testing
 * purposes. It will send strings, CSV-formatted random feature data, like "3,true,-0.135".
 */
public final class ProduceData {

  private static final Logger log = LoggerFactory.getLogger(ProduceData.class);

  private final DatumGenerator<String,String> datumGenerator;
  private final int kafkaPort;
  private final String topic;
  private final int howMany;
  private final int intervalMsec;

  public ProduceData(DatumGenerator<String,String> datumGenerator,
                     int kafkaPort,
                     String topic,
                     int howMany,
                     int intervalMsec) {
    Objects.requireNonNull(datumGenerator);
    Objects.requireNonNull(topic);
    Preconditions.checkArgument(kafkaPort > 0);
    Preconditions.checkArgument(howMany > 0);
    Preconditions.checkArgument(intervalMsec >= 0);
    this.datumGenerator = datumGenerator;
    this.kafkaPort = kafkaPort;
    this.topic = topic;
    this.howMany = howMany;
    this.intervalMsec = intervalMsec;
  }

  public void start() throws InterruptedException {
    RandomGenerator random = RandomManager.getRandom();

    Producer<String,String> producer =
        new Producer<>(new ProducerConfig(ConfigUtils.keyValueToProperties(
            "metadata.broker.list", "localhost:" + kafkaPort,
            "serializer.class", "kafka.serializer.StringEncoder",
            "compression.codec", "gzip",
            "compressed.topics", topic,
            "request.required.acks", 1
            // Above are for Kafka 0.8; following are for 0.9+
            //"bootstrap.servers", "localhost:" + kafkaPort,
            //"key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
            //"value.serializer", "org.apache.kafka.common.serialization.StringSerializer",
            //"compression.type", "gzip",
            //"batch.size", 0,
            //"acks", 1,
            //"max.request.size", 1 << 26 // TODO
        )));
    try {
      for (int i = 0; i < howMany; i++) {
        Pair<String,String> datum = datumGenerator.generate(i, random);
        KeyedMessage<String,String> keyedMessage =
            new KeyedMessage<>(topic, datum.getFirst(), datum.getSecond());
        producer.send(keyedMessage);
        log.debug("Sent datum {} = {}", keyedMessage.key(), keyedMessage.message());
        if (intervalMsec > 0) {
          Thread.sleep(intervalMsec);
        }
      }
    } finally {
      producer.close();
    }
  }

}
