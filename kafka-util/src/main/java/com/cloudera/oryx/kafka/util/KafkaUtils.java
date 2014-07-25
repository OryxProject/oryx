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

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import kafka.admin.AdminUtils;
import kafka.common.TopicExistsException;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class KafkaUtils {

  private static final Logger log = LoggerFactory.getLogger(KafkaUtils.class);

  private static final int ZK_TIMEOUT_MSEC =
      (int) TimeUnit.MILLISECONDS.convert(30, TimeUnit.SECONDS);

  private KafkaUtils() {
  }

  public static void maybeCreateTopic(String host, int port, String topic) {
    ZkClient zkClient = buildClient(host, port);
    if (AdminUtils.topicExists(zkClient, topic)) {
      log.info("No need to create topic {} as it already exists", topic);
    } else {
      log.info("Creating topic {}", topic);
      try {
        AdminUtils.createTopic(zkClient, topic, 1, 1, new Properties());
        log.info("Created Zookeeper topic {}", topic);
      } catch (TopicExistsException tee) {
        log.info("Zookeeper topic {} already exists", topic);
      } finally {
        zkClient.close();
      }
    }
  }

  public static void deleteTopic(String host, int port, String topic) {
    ZkClient zkClient = buildClient(host, port);
    if (AdminUtils.topicExists(zkClient, topic)) {
      log.info("Deleting topic {}", topic);
      try {
        AdminUtils.deleteTopic(zkClient, topic);
        log.info("Deleted Zookeeper topic {}", topic);
      } finally {
        zkClient.close();
      }
    } else {
      log.info("No need to delete topic {} as it does not exist", topic);
    }
  }

  private static ZkClient buildClient(String host, int port) {
    return new ZkClient(host + ':' + port,
                        ZK_TIMEOUT_MSEC,
                        ZK_TIMEOUT_MSEC,
                        ZKStringSerializer$.MODULE$);
  }

}
