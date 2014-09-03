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

package com.cloudera.oryx.lambda.serving;

import com.cloudera.oryx.kafka.util.ProduceData;
import com.cloudera.oryx.kafka.util.RandomDatumGenerator;
import com.cloudera.oryx.lambda.AbstractLambdaIT;
import com.typesafe.config.Config;
import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class AbstractServingIT extends AbstractLambdaIT {

  private static final Logger log = LoggerFactory.getLogger(AbstractServingIT.class);

  private ServingLayer servingLayer;
  private ProduceData updateProducer;

  protected final void startServer(Config config) throws IOException, InterruptedException {
    int bufferMS = WAIT_BUFFER_IN_WRITES * 10;
    Thread.sleep(bufferMS);

    servingLayer = new ServingLayer(config);

    log.info("Starting serving layer");
    servingLayer.start();

    Thread.sleep(bufferMS);
  }

  protected final void startUpdateQueues(
      RandomDatumGenerator<String,String> updateGenerator,
      int howManyUpdate) throws IOException, InterruptedException {

    int zkPort = getZKPort();
    int kafkaPort = getKafkaBrokerPort();

    updateProducer = new ProduceData(updateGenerator,
        zkPort,
        kafkaPort,
        UPDATE_TOPIC,
        howManyUpdate,
        10);

    log.info("Producing updates");
    updateProducer.start();

    int bufferMS = WAIT_BUFFER_IN_WRITES * 10;
    Thread.sleep(bufferMS);
  }

  protected ServingLayer getServingLayer() {
    return servingLayer;
  }

  @After
  public void tearDownServingLayer() {
    if (servingLayer != null) {
      servingLayer.close();
    }
    if (updateProducer != null) {
      updateProducer.deleteTopic();
    }
  }

}
