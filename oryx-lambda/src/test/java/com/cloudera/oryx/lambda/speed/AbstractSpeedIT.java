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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.collection.CloseableIterator;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.lang.LoggingRunnable;
import com.cloudera.oryx.common.lang.LoggingVoidCallable;
import com.cloudera.oryx.kafka.util.ConsumeData;
import com.cloudera.oryx.kafka.util.DefaultCSVDatumGenerator;
import com.cloudera.oryx.kafka.util.ProduceData;
import com.cloudera.oryx.kafka.util.RandomDatumGenerator;
import com.cloudera.oryx.lambda.AbstractLambdaIT;

public abstract class AbstractSpeedIT extends AbstractLambdaIT {

  private static final Logger log = LoggerFactory.getLogger(AbstractSpeedIT.class);

  protected final List<Pair<String,String>> startServerProduceConsumeQueues(
      Config config,
      int howMany,
      int intervalMsec,
      int howManyUpdate,
      int updateIntervalMsec) throws IOException, InterruptedException {
    return startServerProduceConsumeQueues(config,
                                           new DefaultCSVDatumGenerator(),
                                           new MockModelGenerator(),
                                           howMany,
                                           intervalMsec,
                                           howManyUpdate,
                                           updateIntervalMsec);
  }

  protected final List<Pair<String,String>> startServerProduceConsumeQueues(
      Config config,
      RandomDatumGenerator<String,String> inputGenerator,
      RandomDatumGenerator<String,String> updateGenerator,
      int howManyInput,
      int inputIntervalMsec,
      int howManyUpdate,
      int updateIntervalMsec) throws IOException, InterruptedException {

    int zkPort = getZKPort();
    int kakfaPort = getKafkaBrokerPort();

    int bufferMS = WAIT_BUFFER_IN_WRITES * inputIntervalMsec;

    final ProduceData inputProducer = new ProduceData(inputGenerator,
                                                      zkPort,
                                                      kakfaPort,
                                                      INPUT_TOPIC,
                                                      howManyInput,
                                                      inputIntervalMsec);
    final ProduceData updateProducer = new ProduceData(updateGenerator,
                                                       zkPort,
                                                       kakfaPort,
                                                       UPDATE_TOPIC,
                                                       howManyUpdate,
                                                       updateIntervalMsec);

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

      // Sleep for a while after starting server to let it init
      Thread.sleep(bufferMS);

      ExecutorService executor = Executors.newFixedThreadPool(2);
      executor.invokeAll(Arrays.asList(
          new LoggingVoidCallable() {
            @Override
            public void doCall() throws InterruptedException {
              log.info("Producing input");
              inputProducer.start();
            }
          },
          new LoggingVoidCallable() {
            @Override
            public void doCall() throws InterruptedException {
              log.info("Producing updates");
              updateProducer.start();
            }
          }));
      executor.shutdown();

      // Sleep for a while before shutting down server to let it finish
      Thread.sleep(bufferMS);

    } finally {
      inputProducer.deleteTopic();
      updateProducer.deleteTopic();
    }

    return keyMessages;
  }

}
