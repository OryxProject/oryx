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
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import com.typesafe.config.Config;
import org.junit.After;
import org.junit.Before;
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
  protected static final int WAIT_BUFFER_IN_WRITES = 250;

  private int localZKPort;
  private int localKafkaBrokerPort;
  private LocalZKServer localZKServer;
  private LocalKafkaBroker localKafkaBroker;
  private Path batchCheckpointDir;
  private Path speedCheckpointDir;

  @Before
  public final void allocateZKKafkaPorts() throws IOException {
    localZKPort = IOUtils.chooseFreePort();
    localKafkaBrokerPort = IOUtils.chooseFreePort();
  }

  @Before
  public final void setupCheckpointDirs() throws IOException {
    batchCheckpointDir = getTempDir().resolve("batch-checkpoint");
    speedCheckpointDir = getTempDir().resolve("speed-checkpoint");
  }

  protected final void startMessaging() throws IOException, InterruptedException {
    log.info("Starting local test Zookeeper server");
    localZKServer = new LocalZKServer(localZKPort);
    localZKServer.start();
    log.info("Starting local Kafka broker");
    localKafkaBroker = new LocalKafkaBroker(localKafkaBrokerPort, localZKPort);
    localKafkaBroker.start();
    KafkaUtils.deleteTopic("localhost", localZKPort, INPUT_TOPIC);
    KafkaUtils.deleteTopic("localhost", localZKPort, UPDATE_TOPIC);
    KafkaUtils.maybeCreateTopic("localhost", localZKPort, INPUT_TOPIC);
    KafkaUtils.maybeCreateTopic("localhost", localZKPort, UPDATE_TOPIC);
  }

  @After
  public final void tearDownKafkaZK() {
    if (localZKServer != null) {
      KafkaUtils.deleteTopic("localhost", localZKPort, INPUT_TOPIC);
      KafkaUtils.deleteTopic("localhost", localZKPort, UPDATE_TOPIC);
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

  protected Config getConfig() throws IOException {
    Map<String,String> overlay = new HashMap<>();
    String topicBroker = "\"localhost:" + localKafkaBrokerPort + '"';
    String topicLockMaster = "\"localhost:" + localZKPort + '"';
    overlay.put("oryx.input-topic.broker", topicBroker);
    overlay.put("oryx.input-topic.lock.master", topicLockMaster);
    overlay.put("oryx.update-topic.broker", topicBroker);
    overlay.put("oryx.update-topic.lock.master", topicLockMaster);
    String masterLocalAllCores = "\"local[*]\"";
    overlay.put("oryx.batch.streaming.master", masterLocalAllCores);
    overlay.put("oryx.speed.streaming.master", masterLocalAllCores);
    overlay.put("oryx.batch.ui.port", Integer.toString(IOUtils.chooseFreePort()));
    overlay.put("oryx.speed.ui.port", Integer.toString(IOUtils.chooseFreePort()));
    // TODO re-enable checkpointing in tests once it works?
    //ConfigUtils.set(overlay, "oryx.batch.storage.checkpoint-dir", batchCheckpointDir);
    //ConfigUtils.set(overlay, "oryx.speed.storage.checkpoint-dir", speedCheckpointDir);
    return ConfigUtils.overlayOn(overlay, ConfigUtils.getDefault());
  }

  protected final int getZKPort() {
    return localZKPort;
  }

  protected final int getKafkaBrokerPort() {
    return localKafkaBrokerPort;
  }

}
