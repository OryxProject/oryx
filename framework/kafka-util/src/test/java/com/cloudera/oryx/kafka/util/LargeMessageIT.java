/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
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

import java.util.List;

import org.apache.commons.math3.random.RandomGenerator;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.collection.CloseableIterator;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.zk.LocalZKServer;

public final class LargeMessageIT extends OryxTest {

  private static final Logger log = LoggerFactory.getLogger(LargeMessageIT.class);

  private static final String TOPIC = "OryxTest";
  private static final int LARGE_MESSAGE_SIZE = 1 << 25;

  @Test
  public void testProduceConsume() throws Exception {
    int zkPort = IOUtils.chooseFreePort();
    int kafkaBrokerPort = IOUtils.chooseFreePort();
    try (LocalZKServer localZKServer = new LocalZKServer(zkPort);
         LocalKafkaBroker localKafkaBroker = new LocalKafkaBroker(kafkaBrokerPort, zkPort)) {

      localZKServer.start();
      localKafkaBroker.start();

      String zkHostPort = "localhost:" + zkPort;
      KafkaUtils.deleteTopic(zkHostPort, TOPIC);
      KafkaUtils.maybeCreateTopic(zkHostPort, TOPIC, ConfigUtils.keyValueToProperties(
          "max.message.bytes", LARGE_MESSAGE_SIZE + (1 << 15) // Overhead for Kafka message metadata, key
      ));

      ProduceData produce = new ProduceData(new BigDatumGenerator(),
                                            zkPort,
                                            localKafkaBroker.getPort(),
                                            TOPIC,
                                            1,
                                            0);

      List<Pair<String,String>> keyMessages;
      try (CloseableIterator<Pair<String,String>> data = new ConsumeData(TOPIC, zkPort).iterator()) {

        log.info("Starting consumer thread");
        ConsumeTopicRunnable consumeTopic = new ConsumeTopicRunnable(data);
        new Thread(consumeTopic, "ConsumeTopicThread").start();

        // Sleep to let consumer start
        sleepSeconds(3);

        log.info("Producing data");
        produce.start();

        // Sleep for a while before shutting down producer to let both finish
        sleepSeconds(3);

        keyMessages = consumeTopic.getKeyMessages();
      } finally {
        KafkaUtils.deleteTopic(zkHostPort, TOPIC);
      }

      assertEquals(1, keyMessages.size());
      assertEquals(BigDatumGenerator.LARGE_MESSAGE, keyMessages.get(0).getSecond());
    }
  }

  private static final class BigDatumGenerator implements DatumGenerator<String,String> {
    private static String LARGE_MESSAGE;
    @Override
    public Pair<String,String> generate(int id, RandomGenerator random) {
      if (LARGE_MESSAGE == null) {
        StringBuilder builder = new StringBuilder(LARGE_MESSAGE_SIZE);
        for (int i = 0; i < LARGE_MESSAGE_SIZE; i++) {
          builder.append((char) (' ' + random.nextInt('~' - ' ' + 1)));
        }
        LARGE_MESSAGE = builder.toString();
      }
      return new Pair<>("", LARGE_MESSAGE);
    }
  }

}
