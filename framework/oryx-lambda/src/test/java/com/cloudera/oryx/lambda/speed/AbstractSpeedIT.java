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
import java.util.List;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.api.KeyMessage;
import com.cloudera.oryx.common.collection.CloseableIterator;
import com.cloudera.oryx.common.lang.LoggingCallable;
import com.cloudera.oryx.kafka.util.ConsumeData;
import com.cloudera.oryx.kafka.util.ConsumeTopicRunnable;
import com.cloudera.oryx.kafka.util.DefaultCSVDatumGenerator;
import com.cloudera.oryx.kafka.util.ProduceData;
import com.cloudera.oryx.kafka.util.DatumGenerator;
import com.cloudera.oryx.lambda.AbstractLambdaIT;

public abstract class AbstractSpeedIT extends AbstractLambdaIT {

  private static final Logger log = LoggerFactory.getLogger(AbstractSpeedIT.class);

  final List<KeyMessage<String,String>> startServerProduceConsumeTopics(
      Config config,
      int howMany,
      int howManyUpdate) throws IOException, InterruptedException {
    return startServerProduceConsumeTopics(config,
                                           new DefaultCSVDatumGenerator(),
                                           new MockModelGenerator(),
                                           howMany,
                                           howManyUpdate);
  }

  protected final List<KeyMessage<String,String>> startServerProduceConsumeTopics(
      Config config,
      DatumGenerator<String,String> inputGenerator,
      DatumGenerator<String,String> updateGenerator,
      int howManyInput,
      int howManyUpdate) throws InterruptedException {

    int zkPort = getZKPort();
    int kafkaPort = getKafkaBrokerPort();

    ProduceData inputProducer = new ProduceData(inputGenerator,
                                                kafkaPort,
                                                INPUT_TOPIC,
                                                howManyInput,
                                                10);
    ProduceData updateProducer = new ProduceData(updateGenerator,
                                                 kafkaPort,
                                                 UPDATE_TOPIC,
                                                 howManyUpdate,
                                                 10);

    List<KeyMessage<String,String>> keyMessages;
    try (CloseableIterator<KeyMessage<String,String>> data =
             new ConsumeData(UPDATE_TOPIC, zkPort).iterator();
         SpeedLayer<?,?,?> speedLayer = new SpeedLayer<>(config)) {

      log.info("Starting speed layer");
      speedLayer.start();

      // Sleep to let speed layer start
      sleepSeconds(3);

      log.info("Starting consumer thread");
      ConsumeTopicRunnable consumeUpdate = new ConsumeTopicRunnable(data);
      new Thread(LoggingCallable.log(consumeUpdate).asRunnable(), "ConsumeUpdateThread").start();

      consumeUpdate.awaitRun();

      // Load all updates first
      log.info("Producing updates");
      updateProducer.start();

      // Sleep generation to make sure updates are digested
      int genIntervalSec = config.getInt("oryx.speed.streaming.generation-interval-sec");
      sleepSeconds(genIntervalSec);

      log.info("Producing input");
      inputProducer.start();

      // Sleep generation before shutting down server to let it finish
      sleepSeconds(genIntervalSec);

      keyMessages = consumeUpdate.getKeyMessages();
    }

    return keyMessages;
  }

}
