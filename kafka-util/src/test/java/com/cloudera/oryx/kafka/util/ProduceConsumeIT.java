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

import java.util.Collection;
import java.util.HashSet;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.collection.CloseableIterator;
import com.cloudera.oryx.zk.LocalZKServer;

/**
 * Tests {@link ProduceData} and {@link ConsumeData} together.
 */
public final class ProduceConsumeIT extends OryxTest {

  private static final Logger log = LoggerFactory.getLogger(ProduceConsumeIT.class);

  private static final String TOPIC = "OryxTest";
  private static final int NUM_DATA = 100;

  @Test
  public void testProduceConsume() throws Exception {
    try (LocalZKServer localZKServer = new LocalZKServer();
         LocalKafkaBroker localKafkaBroker = new LocalKafkaBroker()) {

      localZKServer.start();
      localKafkaBroker.start();

      int zkPort = localZKServer.getPort();
      KafkaUtils.deleteTopic("localhost", zkPort, TOPIC);
      KafkaUtils.maybeCreateTopic("localhost", zkPort, TOPIC);

      ProduceData produce = new ProduceData(new DefaultCSVDatumGenerator(),
                                            zkPort,
                                            localKafkaBroker.getPort(),
                                            TOPIC,
                                            NUM_DATA,
                                            50);

      final Collection<Integer> keys = new HashSet<>();

      try (CloseableIterator<String[]> data = new ConsumeData(TOPIC, zkPort).iterator()) {

        new Thread(new Runnable() {
          @Override
          public void run() {
            while (data.hasNext()) {
              keys.add(Integer.valueOf(data.next()[0]));
            }
          }
        }).start();

        // Sleep for a while after starting consumer to let it init
        Thread.sleep(1000L);

        log.info("Producing data");
        produce.start();

        // Sleep for a while before shutting down producer to let both finish
        Thread.sleep(1000L);

      } finally {
        KafkaUtils.deleteTopic("localhost", zkPort, TOPIC);
      }

      assertEquals(NUM_DATA, keys.size());
      for (int i = 0; i < NUM_DATA; i++) {
        assertTrue(keys.contains(i));
      }
    }
  }

}
