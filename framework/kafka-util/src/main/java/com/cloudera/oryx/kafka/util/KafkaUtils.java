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

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;
import kafka.utils.ZkUtils$;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.collection.Pair;

/**
 * Kafka-related utility methods.
 */
public final class KafkaUtils {

  private static final Logger log = LoggerFactory.getLogger(KafkaUtils.class);

  private static final int ZK_TIMEOUT_MSEC =
      (int) TimeUnit.MILLISECONDS.convert(30, TimeUnit.SECONDS);

  private KafkaUtils() {}

  /**
   * @param zkServers Zookeeper server string: host1:port1[,host2:port2,...]
   * @param topic topic to create (if not already existing)
   * @param partitions number of topic partitions
   */
  public static void maybeCreateTopic(String zkServers, String topic, int partitions) {
    maybeCreateTopic(zkServers, topic, partitions, new Properties());
  }

  /**
   * @param zkServers Zookeeper server string: host1:port1[,host2:port2,...]
   * @param topic topic to create (if not already existing)
   * @param partitions number of topic partitions
   * @param topicProperties optional topic config properties
   */
  public static void maybeCreateTopic(String zkServers,
                                      String topic,
                                      int partitions,
                                      Properties topicProperties) {
    ZkUtils zkUtils = ZkUtils.apply(zkServers, ZK_TIMEOUT_MSEC, ZK_TIMEOUT_MSEC, false);
    try {
      if (AdminUtils.topicExists(zkUtils, topic)) {
        log.info("No need to create topic {} as it already exists", topic);
      } else {
        log.info("Creating topic {} with {} partition(s)", topic, partitions);
        try {
          AdminUtils.createTopic(
              zkUtils, topic, partitions, 1, topicProperties, RackAwareMode.Enforced$.MODULE$);
          log.info("Created topic {}", topic);
        } catch (TopicExistsException re) {
          log.info("Topic {} already exists", topic);
        }
      }
    } finally {
      zkUtils.close();
    }
  }

  /**
   * @param zkServers Zookeeper server string: host1:port1[,host2:port2,...]
   * @param topic topic to check for existence
   * @return {@code true} if and only if the given topic exists
   */
  public static boolean topicExists(String zkServers, String topic) {
    ZkUtils zkUtils = ZkUtils.apply(zkServers, ZK_TIMEOUT_MSEC, ZK_TIMEOUT_MSEC, false);
    try {
      return AdminUtils.topicExists(zkUtils, topic);
    } finally {
      zkUtils.close();
    }
  }

  /**
   * @param zkServers Zookeeper server string: host1:port1[,host2:port2,...]
   * @param topic topic to delete, if it exists
   */
  public static void deleteTopic(String zkServers, String topic) {
    ZkUtils zkUtils = ZkUtils.apply(zkServers, ZK_TIMEOUT_MSEC, ZK_TIMEOUT_MSEC, false);
    try {
      if (AdminUtils.topicExists(zkUtils, topic)) {
        log.info("Deleting topic {}", topic);
        AdminUtils.deleteTopic(zkUtils, topic);
        log.info("Deleted Zookeeper topic {}", topic);
      } else {
        log.info("No need to delete topic {} as it does not exist", topic);
      }
    } finally {
      zkUtils.close();
    }
  }

  /**
   * @param zkServers Zookeeper server string: host1:port1[,host2:port2,...]
   * @param groupID consumer group to update
   * @param offsets mapping of (topic and) partition to offset to push to Zookeeper
   */
  public static void setOffsets(String zkServers,
                                String groupID,
                                Map<Pair<String,Integer>,Long> offsets) {
    ZkUtils zkUtils = ZkUtils.apply(zkServers, ZK_TIMEOUT_MSEC, ZK_TIMEOUT_MSEC, false);
    try {
      offsets.forEach((topicAndPartition, offset) -> {
        String topic = topicAndPartition.getFirst();
        int partition = topicAndPartition.getSecond();
        String partitionOffsetPath = "/consumers/" + groupID + "/offsets/" + topic + "/" + partition;
        zkUtils.updatePersistentPath(partitionOffsetPath,
                                     Long.toString(offset),
                                     ZkUtils$.MODULE$.defaultAcls(false, ""));
      });
    } finally {
      zkUtils.close();
    }
  }

}
