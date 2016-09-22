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
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.zookeeper.server.DatadirCleanupManager;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.lang.JVMUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;

/**
 * This class contains code copied from Zookeeper's QuorumPeerMain and ZooKeeperServerMain.
 * It runs a local instance of Zookeeper, which can be useful for testing.
 */
public final class LocalZKServer implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(LocalZKServer.class);

  private final int port;
  private Path dataDir;
  private DatadirCleanupManager purgeManager;
  private ZooKeeperServer zkServer;
  private FileTxnSnapLog transactionLog;
  private ServerCnxnFactory connectionFactory;
  private volatile boolean closed;

  /**
   * Creates an instance that will run Zookeeper on the given port.
   *
   * @param port port on which Zookeeper will listen
   */
  public LocalZKServer(int port) {
    this.port = port;
  }

  /**
   * Starts Zookeeper.
   *
   * @throws IOException if an error occurs during initialization
   * @throws InterruptedException if an error occurs during initialization
   */
  public synchronized void start() throws IOException, InterruptedException {
    log.info("Starting Zookeeper on port {}", port);

    dataDir = Files.createTempDirectory(LocalZKServer.class.getSimpleName());
    dataDir.toFile().deleteOnExit();

    QuorumPeerConfig quorumConfig = new QuorumPeerConfig();
    try {
      quorumConfig.parseProperties(ConfigUtils.keyValueToProperties(
          "dataDir", dataDir.toAbsolutePath(),
          "clientPort", port
      ));
    } catch (QuorumPeerConfig.ConfigException e) {
      throw new IllegalArgumentException(e);
    }

    purgeManager =
        new DatadirCleanupManager(quorumConfig.getDataDir(),
                                  quorumConfig.getDataLogDir(),
                                  quorumConfig.getSnapRetainCount(),
                                  quorumConfig.getPurgeInterval());
    purgeManager.start();

    ServerConfig serverConfig = new ServerConfig();
    serverConfig.readFrom(quorumConfig);

    zkServer = new ZooKeeperServer();
    zkServer.setTickTime(serverConfig.getTickTime());
    zkServer.setMinSessionTimeout(serverConfig.getMinSessionTimeout());
    zkServer.setMaxSessionTimeout(serverConfig.getMaxSessionTimeout());

    // These two ServerConfig methods returned String in 3.4.x and File in 3.5.x
    transactionLog = new FileTxnSnapLog(new File(serverConfig.getDataLogDir().toString()),
                                        new File(serverConfig.getDataDir().toString()));
    zkServer.setTxnLogFactory(transactionLog);

    connectionFactory = ServerCnxnFactory.createFactory();
    connectionFactory.configure(serverConfig.getClientPortAddress(), serverConfig.getMaxClientCnxns());
    connectionFactory.startup(zkServer);
  }

  /**
   * Blocks until Zookeeper terminates.
   *
   * @throws InterruptedException if the caller thread is interrupted while waiting
   */
  private void await() throws InterruptedException {
    connectionFactory.join();
  }

  /**
   * Shuts down Zookeeper.
   */
  @Override
  public synchronized void close() throws IOException {
    log.info("Closing...");
    if (closed) {
      return;
    }
    closed = true;
    if (connectionFactory != null) {
      connectionFactory.shutdown();
      connectionFactory = null;
    }
    if (zkServer != null) {
      zkServer.shutdown();
      zkServer = null;
    }
    if (transactionLog != null) {
      transactionLog.close();
      transactionLog = null;
    }
    if (purgeManager != null) {
      purgeManager.shutdown();
      purgeManager = null;
    }
    if (dataDir != null) {
      IOUtils.deleteRecursively(dataDir);
      dataDir = null;
    }
  }

  public static void main(String[] args) throws Exception {
    int port = args.length > 0 ? Integer.parseInt(args[0]) : IOUtils.chooseFreePort();
    try (final LocalZKServer zkServer = new LocalZKServer(port)) {
      JVMUtils.closeAtShutdown(zkServer);
      zkServer.start();
      zkServer.await();
    }
  }

}
