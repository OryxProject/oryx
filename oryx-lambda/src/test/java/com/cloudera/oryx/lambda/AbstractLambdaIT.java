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

package com.cloudera.oryx.lambda;

import java.io.IOException;

import com.typesafe.config.Config;
import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.kafka.util.KafkaUtils;
import com.cloudera.oryx.kafka.util.LocalKafkaBroker;
import com.cloudera.oryx.zk.LocalZKServer;

/**
 * Superclass of test cases for this module. Takes care of setting up services like
 * Kafka and Zookeeper, and stopping them.
 */
public abstract class AbstractLambdaIT extends OryxTest {

  private static final Logger log = LoggerFactory.getLogger(AbstractLambdaIT.class);

  protected static final String INPUT_TOPIC = "OryxInput";
  protected static final String UPDATE_TOPIC = "OryxUpdate";

  private LocalZKServer localZKServer;
  private LocalKafkaBroker localKafkaBroker;

  protected final void startMessageQueue() throws IOException, InterruptedException {
    log.info("Starting local test Zookeeper server");
    localZKServer = new LocalZKServer();
    localZKServer.start();
    log.info("Starting local Kafka broker");
    localKafkaBroker = new LocalKafkaBroker();
    localKafkaBroker.start();
    int zkPort = localZKServer.getPort();
    KafkaUtils.deleteTopic("localhost", zkPort, INPUT_TOPIC);
    KafkaUtils.deleteTopic("localhost", zkPort, UPDATE_TOPIC);
    KafkaUtils.maybeCreateTopic("localhost", zkPort, INPUT_TOPIC);
    KafkaUtils.maybeCreateTopic("localhost", zkPort, UPDATE_TOPIC);
  }

  @After
  public final void tearDownKafkaZK() {
    if (localZKServer != null) {
      int zkPort = localZKServer.getPort();
      KafkaUtils.deleteTopic("localhost", zkPort, INPUT_TOPIC);
      KafkaUtils.deleteTopic("localhost", zkPort, UPDATE_TOPIC);
    }
    if (localKafkaBroker != null) {
      log.info("Stopping Kafka");
      IOUtils.closeQuietly(localKafkaBroker);
      localKafkaBroker = null;
    }
    if (localZKServer != null) {
      log.info("Stopping Zookeeper");
      IOUtils.closeQuietly(localZKServer);
      localZKServer = null;
    }
  }

  protected Config getConfig() {
    return ConfigUtils.getDefault();
  }

  protected final int getZKPort() {
    return localZKServer.getPort();
  }

  protected final int getKafkaBrokerPort() {
    return localKafkaBroker.getPort();
  }

}
