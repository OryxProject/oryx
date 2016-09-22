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

import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.api.KeyMessage;
import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.collection.CloseableIterator;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.lang.LoggingCallable;

/**
 * Tests {@link ProduceData} and {@link ConsumeData} together.
 */
public final class ProduceConsumeIT extends OryxTest {

  private static final Logger log = LoggerFactory.getLogger(ProduceConsumeIT.class);

  private static final String TOPIC = ProduceConsumeIT.class.getSimpleName() + "Topic";
  private static final int NUM_DATA = 100;

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
      KafkaUtils.maybeCreateTopic(zkHostPort, TOPIC, 4);

      ProduceData produce = new ProduceData(new DefaultCSVDatumGenerator(),
                                            localKafkaBroker.getPort(),
                                            TOPIC,
                                            NUM_DATA,
                                            0);

      List<String> keys;
      try (CloseableIterator<KeyMessage<String,String>> data = new ConsumeData(TOPIC, zkPort).iterator()) {

        log.info("Starting consumer thread");
        ConsumeTopicRunnable consumeTopic = new ConsumeTopicRunnable(data, NUM_DATA);
        new Thread(LoggingCallable.log(consumeTopic).asRunnable(), "ConsumeTopicThread").start();

        consumeTopic.awaitRun();

        log.info("Producing data");
        produce.start();

        consumeTopic.awaitMessages();
        keys = consumeTopic.getKeys();
      } finally {
        KafkaUtils.deleteTopic(zkHostPort, TOPIC);
      }

      if (keys.size() != NUM_DATA) {
        log.info("keys = {}", keys);
        assertEquals(NUM_DATA, keys.size());
      }
      for (int i = 0; i < NUM_DATA; i++) {
        assertContains(keys, Integer.toString(i));
      }
    }
  }

}
