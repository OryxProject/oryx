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

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.io.IOUtils;

public final class KafkaUtilsIT extends OryxTest {

  private static final String TOPIC = KafkaUtilsIT.class.getSimpleName() + "Topic";
  private static final String GROUP = KafkaUtilsIT.class.getSimpleName() + "Group";

  @Test
  public void testGetSetOffsets() throws Exception {
    int zkPort = IOUtils.chooseFreePort();
    int kafkaBrokerPort = IOUtils.chooseFreePort();
    try (LocalZKServer localZKServer = new LocalZKServer(zkPort);
         LocalKafkaBroker localKafkaBroker = new LocalKafkaBroker(kafkaBrokerPort, zkPort)) {

      localZKServer.start();
      localKafkaBroker.start();

      String zkHostPort = "localhost:" + zkPort;
      KafkaUtils.maybeCreateTopic(zkHostPort, TOPIC, 2);

      Thread.sleep(2000L);

      try {
        Map<Pair<String,Integer>,Long> initialOffsets = KafkaUtils.getOffsets(zkHostPort, GROUP, TOPIC);
        assertEquals(2, initialOffsets.size());
        initialOffsets.values().forEach(Assert::assertNull);

        Map<Pair<String,Integer>,Long> updatedOffsets = new HashMap<>(2);
        updatedOffsets.put(new Pair<>(TOPIC, 0), 123L);
        updatedOffsets.put(new Pair<>(TOPIC, 1), 456L);
        KafkaUtils.setOffsets(zkHostPort, GROUP, updatedOffsets);

        Map<Pair<String,Integer>,Long> newOffsets = KafkaUtils.getOffsets(zkHostPort, GROUP, TOPIC);
        assertEquals(2, newOffsets.size());
        assertEquals(123L, newOffsets.get(new Pair<>(TOPIC, 0)).longValue());
        assertEquals(456L, newOffsets.get(new Pair<>(TOPIC, 1)).longValue());

      } finally {
        KafkaUtils.deleteTopic(zkHostPort, TOPIC);
      }

    }
  }

  @Test
  public void testFillInLatestOffsets() throws Exception {
    int zkPort = IOUtils.chooseFreePort();
    int kafkaBrokerPort = IOUtils.chooseFreePort();
    try (LocalZKServer localZKServer = new LocalZKServer(zkPort);
         LocalKafkaBroker localKafkaBroker = new LocalKafkaBroker(kafkaBrokerPort, zkPort)) {

      localZKServer.start();
      localKafkaBroker.start();

      String zkHostPort = "localhost:" + zkPort;

      KafkaUtils.maybeCreateTopic(zkHostPort, TOPIC, 2);

      String kafkaBrokerHostPort = "localhost:" + kafkaBrokerPort;

      Map<String, String> kafkaParams = new HashMap<>();
      kafkaParams.put("zookeeper.connect", zkHostPort); // needed for SimpleConsumer later
      kafkaParams.put("group.id", GROUP);
      kafkaParams.put("metadata.broker.list", kafkaBrokerHostPort);
      // Newer version of metadata.broker.list:
      kafkaParams.put("bootstrap.servers", kafkaBrokerHostPort);

      Thread.sleep(2000L);

      Pair<String,Integer> topic0 = new Pair<>(TOPIC, 0);
      Pair<String,Integer> topic1 = new Pair<>(TOPIC, 1);

      try {
        Map<Pair<String,Integer>,Long> offsets = new HashMap<>();
        offsets.put(topic0, 0L);
        offsets.put(topic1, null);

        KafkaUtils.fillInLatestOffsets(offsets, kafkaParams);

        assertEquals(0L, offsets.get(topic0).longValue());
        assertEquals(0L, offsets.get(topic1).longValue());

        offsets.clear();
        offsets.put(topic0, 1000L);
        offsets.put(topic1, -1000L);

        KafkaUtils.fillInLatestOffsets(offsets, kafkaParams);

        assertEquals(0L, offsets.get(topic0).longValue());
        assertEquals(0L, offsets.get(topic1).longValue());
      } finally {
        KafkaUtils.deleteTopic(zkHostPort, TOPIC);
      }
    }
  }

}
