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

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.lang.JVMUtils;

/**
 * Runs a local instance of the Kafka broker. Useful for testing.
 */
public final class LocalKafkaBroker implements Closeable {

  public static final int TEST_BROKER_ID = 8675309;

  private static final Logger log = LoggerFactory.getLogger(LocalKafkaBroker.class);

  private final int port;
  private final int zkPort;
  private Path logsDir;
  private KafkaServerStartable kafkaServer;

  /**
   * Creates an instance that will listen on the given port and connect to the given
   * Zookeeper port.
   *
   * @param port port for Kafka broker to listen on
   * @param zkPort port on which Zookeeper is listening
   */
  public LocalKafkaBroker(int port, int zkPort) {
    this.port = port;
    this.zkPort = zkPort;
  }

  public int getPort() {
    return port;
  }

  /**
   * Starts the Kafka broker.
   *
   * @throws IOException if an error occurs during initialization
   */
  public synchronized void start() throws IOException {
    log.info("Starting Kafka broker on port {}", port);

    logsDir = Files.createTempDirectory(LocalKafkaBroker.class.getSimpleName());
    logsDir.toFile().deleteOnExit();

    Properties properties = new Properties();
    properties.setProperty("broker.id", Integer.toString(TEST_BROKER_ID));
    properties.setProperty("log.dirs", logsDir.toAbsolutePath().toString());
    properties.setProperty("port", Integer.toString(port));
    properties.setProperty("zookeeper.connect", "localhost:" + zkPort);

    kafkaServer = new KafkaServerStartable(new KafkaConfig(properties));
    kafkaServer.startup();
  }

  /**
   * Blocks until the Kafka broker terminates.
   */
  public void await() {
    kafkaServer.awaitShutdown();
  }

  /**
   * Stops the Kafka broker.
   */
  @Override
  public synchronized void close() throws IOException {
    log.info("Closing...");
    if (kafkaServer != null) {
      kafkaServer.shutdown();
      kafkaServer = null;
    }
    if (logsDir != null) {
      IOUtils.deleteRecursively(logsDir);
      logsDir = null;
    }
  }

  public static void main(String[] args) throws Exception {
    int port = args.length > 0 ? Integer.parseInt(args[0]) : IOUtils.chooseFreePort();
    int zkPort = args.length > 1 ? Integer.parseInt(args[1]) : IOUtils.chooseFreePort();
    try (final LocalKafkaBroker kafkaBroker = new LocalKafkaBroker(port, zkPort)) {
      JVMUtils.closeAtShutdown(kafkaBroker);
      kafkaBroker.start();
      kafkaBroker.await();
    }
  }

}
