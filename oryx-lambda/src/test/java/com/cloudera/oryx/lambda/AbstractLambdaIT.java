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
import java.util.List;

import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.kafka.util.DefaultCSVDatumGenerator;
import com.cloudera.oryx.kafka.util.KafkaUtils;
import com.cloudera.oryx.kafka.util.LocalKafkaBroker;
import com.cloudera.oryx.kafka.util.ProduceData;
import com.cloudera.oryx.kafka.util.RandomDatumGenerator;
import com.cloudera.oryx.zk.LocalZKServer;

/**
 * Superclass of test cases for this module. Takes care of setting up services like
 * Kafka and Zookeeper, and stopping them.
 */
public abstract class AbstractLambdaIT extends OryxTest {

  private static final Logger log = LoggerFactory.getLogger(AbstractLambdaIT.class);

  private static final int WAIT_BUFFER_IN_WRITES = 250;

  private LocalZKServer localZKServer;
  private LocalKafkaBroker localKafkaBroker;

  protected final void startMessageQueue() throws IOException, InterruptedException {
    log.info("Starting local test Zookeeper server");
    localZKServer = new LocalZKServer();
    localZKServer.start();
    log.info("Starting local Kafka broker");
    localKafkaBroker = new LocalKafkaBroker();
    localKafkaBroker.start();
    KafkaUtils.maybeCreateTopic("localhost", localZKServer.getPort(), "OryxInput");
    KafkaUtils.maybeCreateTopic("localhost", localZKServer.getPort(), "OryxUpdate");
  }

  @After
  public void tearDownTestState() {
    if (localKafkaBroker != null) {
      log.info("Stopping Kafka");
      IOUtils.closeQuietly(localKafkaBroker);
      localKafkaBroker = null;
    }
    if (localZKServer != null) {
      //KafkaUtils.deleteTopic("localhost", localZKServer.getPort(), "OryxInput");
      //KafkaUtils.deleteTopic("localhost", localZKServer.getPort(), "OryxUpdate");
      log.info("Stopping Zookeeper");
      IOUtils.closeQuietly(localZKServer);
      localZKServer = null;
    }
  }

  protected static void startServerAndSendData(Config config, int howMany, int intervalMsec)
      throws InterruptedException {
    startServerAndSendData(config, new DefaultCSVDatumGenerator(), howMany, intervalMsec);
  }

  protected static void startServerAndSendData(Config config,
                                               RandomDatumGenerator<String> datumGenerator,
                                               int howMany,
                                               int intervalMsec) throws InterruptedException {
    int bufferMS = WAIT_BUFFER_IN_WRITES * intervalMsec;
    ProduceData produce = new ProduceData(datumGenerator, howMany, intervalMsec);
    try (Server<?,?,?> server = new Server<>(config)) {
      log.info("Starting server");
      server.start();
      // Sleep for a while after starting server to let it init
      Thread.sleep(bufferMS);
      log.info("Producing data");
      produce.start();
      // Sleep for a while before shutting down server to let it finish
      Thread.sleep(bufferMS);
    } finally {
      produce.deleteTopic();
    }
  }

  protected static void checkOutputData(Path dataDir, int expectedCount) throws IOException {
    List<Path> dataFiles = IOUtils.listFiles(dataDir, "*/part-*");
    Configuration hadoopConf = new Configuration();
    Writable key = new Text();
    Writable message = new Text();
    int count = 0;
    for (Path dataFile : dataFiles) {
      log.debug("Data file: {}", dataFile);
      SequenceFile.Reader.Option fileOptions =
          SequenceFile.Reader.file(new org.apache.hadoop.fs.Path(dataFile.toUri().toString()));
      try (SequenceFile.Reader reader = new SequenceFile.Reader(hadoopConf, fileOptions)) {
        while (reader.next(key, message)) {
          log.debug("  {} = {}", key, message);
          count++;
        }
      }
    }
    log.info("Read {} data", count);
    assertEquals(expectedCount, count);
  }

  protected static void checkIntervals(int numIntervals,
                                       int dataToWrite,
                                       int writeIntervalMsec,
                                       int genIntervalSec) {
    int minExpectedIntervals =
        (dataToWrite + 2 * WAIT_BUFFER_IN_WRITES) * writeIntervalMsec / (1000 * genIntervalSec);
    // Can be up to +4 depending on distribution and timing, and because the framework
    // waits a while before starting to send data and after last data is sent before
    // stopping the streaming system
    assertTrue(
        "Expected " + minExpectedIntervals +
        " intervals (or up to +4 more) but got " + numIntervals,
        numIntervals >= minExpectedIntervals && numIntervals <= minExpectedIntervals + 4);
  }

}
