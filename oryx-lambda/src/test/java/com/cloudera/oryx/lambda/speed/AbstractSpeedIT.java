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

package com.cloudera.oryx.lambda.speed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.collection.CloseableIterator;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.lang.LoggingRunnable;
import com.cloudera.oryx.kafka.util.ConsumeData;
import com.cloudera.oryx.kafka.util.DefaultCSVDatumGenerator;
import com.cloudera.oryx.kafka.util.ProduceData;
import com.cloudera.oryx.kafka.util.RandomDatumGenerator;
import com.cloudera.oryx.lambda.AbstractLambdaIT;

public abstract class AbstractSpeedIT extends AbstractLambdaIT {

  private static final Logger log = LoggerFactory.getLogger(AbstractSpeedIT.class);

  protected static final int WAIT_BUFFER_IN_WRITES = 25;

  protected final List<Pair<String,String>> startServerProduceConsumeQueues(
      Config config,
      int howMany,
      int howManyUpdate) throws IOException, InterruptedException {
    return startServerProduceConsumeQueues(config,
                                           new DefaultCSVDatumGenerator(),
                                           new MockModelGenerator(),
                                           howMany,
                                           howManyUpdate);
  }

  protected final List<Pair<String,String>> startServerProduceConsumeQueues(
      Config config,
      RandomDatumGenerator<String,String> inputGenerator,
      RandomDatumGenerator<String,String> updateGenerator,
      int howManyInput,
      int howManyUpdate) throws IOException, InterruptedException {

    int zkPort = getZKPort();
    int kafkaPort = getKafkaBrokerPort();

    int bufferMS = WAIT_BUFFER_IN_WRITES * 10;

    ProduceData inputProducer = new ProduceData(inputGenerator,
                                                zkPort,
                                                kafkaPort,
                                                INPUT_TOPIC,
                                                howManyInput,
                                                10);
    ProduceData updateProducer = new ProduceData(updateGenerator,
                                                 zkPort,
                                                 kafkaPort,
                                                 UPDATE_TOPIC,
                                                 howManyUpdate,
                                                 10);

    final List<Pair<String,String>> keyMessages = new ArrayList<>();

    Thread.sleep(bufferMS);

    try (CloseableIterator<Pair<String,String>> data =
             new ConsumeData(UPDATE_TOPIC, zkPort).iterator();
         SpeedLayer<?,?,?> speedLayer = new SpeedLayer<>(config)) {

      log.info("Starting consumer thread");
      new Thread(new LoggingRunnable() {
        @Override
        public void doRun() {
          while (data.hasNext()) {
            keyMessages.add(data.next());
          }
        }
      }).start();

      log.info("Starting speed layer");
      speedLayer.start();

      Thread.sleep(bufferMS);

      // Load all updates first
      log.info("Producing updates");
      updateProducer.start();

      // Sleep for a while after starting server to let it init
      Thread.sleep(bufferMS);

      log.info("Producing input");
      inputProducer.start();

      // Sleep for a while before shutting down server to let it finish
      Thread.sleep(bufferMS);

    } finally {
      inputProducer.deleteTopic();
      updateProducer.deleteTopic();
    }

    return keyMessages;
  }

}
