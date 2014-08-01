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

import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.zk.LocalZKServer;

public final class ProduceConsumeIT extends OryxTest {

  private static final String TOPIC = "OryxTest";

  @Test
  public void testProduceConsume() throws Exception {
    try (LocalZKServer localZKServer = new LocalZKServer();
         LocalKafkaBroker localKafkaBroker = new LocalKafkaBroker()) {
      localZKServer.start();
      localKafkaBroker.start();
      KafkaUtils.deleteTopic("localhost", localZKServer.getPort(), TOPIC);
      KafkaUtils.maybeCreateTopic("localhost", localZKServer.getPort(), TOPIC);
      try {
        final Collection<Integer> count = new HashSet<>();
        new Thread(new Runnable() {
          @Override
          public void run() {
            for (String[] km : new ConsumeData(TOPIC, localZKServer.getPort())) {
              count.add(Integer.valueOf(km[0]));
            }
          }
        }).start();
        Thread.sleep(2000L);
        new ProduceData(localZKServer.getPort(), localKafkaBroker.getPort(), TOPIC).start();
        Thread.sleep(2000L);
        assertEquals(ProduceData.DEFAULT_HOW_MANY, count.size());
      } finally {
        KafkaUtils.deleteTopic("localhost", localZKServer.getPort(), TOPIC);
      }
    }
  }

}
