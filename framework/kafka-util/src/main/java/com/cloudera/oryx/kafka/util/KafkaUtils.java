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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import kafka.admin.AdminUtils;
import kafka.common.TopicAndPartition;
import kafka.common.TopicExistsException;
import kafka.utils.ZKGroupTopicDirs;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.JavaConversions;

public final class KafkaUtils {

  private static final Logger log = LoggerFactory.getLogger(KafkaUtils.class);

  private static final int ZK_TIMEOUT_MSEC =
      (int) TimeUnit.MILLISECONDS.convert(30, TimeUnit.SECONDS);

  private KafkaUtils() {
  }

  /**
   * @param zkServers Zookeeper server string: host1:port1[,host2:port2,...]
   * @param topic topic to create (if not already existing)
   */
  public static void maybeCreateTopic(String zkServers, String topic) {
    try (AutoZkClient zkClient = new AutoZkClient(zkServers)) {
      if (AdminUtils.topicExists(zkClient, topic)) {
        log.info("No need to create topic {} as it already exists", topic);
      } else {
        log.info("Creating topic {}", topic);
        try {
          AdminUtils.createTopic(zkClient, topic, 1, 1, new Properties());
          log.info("Created Zookeeper topic {}", topic);
        } catch (TopicExistsException tee) {
          log.info("Zookeeper topic {} already exists", topic);
        }
      }
    }
  }

  /**
   * @param zkServers Zookeeper server string: host1:port1[,host2:port2,...]
   * @param topic topic to delete, if it exists
   */
  public static void deleteTopic(String zkServers, String topic) {
    try (AutoZkClient zkClient = new AutoZkClient(zkServers)) {
      if (AdminUtils.topicExists(zkClient, topic)) {
        log.info("Deleting topic {}", topic);
        try {
          AdminUtils.deleteTopic(zkClient, topic);
          log.info("Deleted Zookeeper topic {}", topic);
        } catch (ZkNodeExistsException nee) {
          log.info("Delete was already scheduled for Zookeeper topic {}", topic);
        }
      } else {
        log.info("No need to delete topic {} as it does not exist", topic);
      }
    }
  }

  /**
   * @param zkServers Zookeeper server string: host1:port1[,host2:port2,...]
   * @param groupID consumer group to get offsets for
   * @param topic topic to get offsets for
   * @return mapping of (topic and) partition to offset
   */
  public static Map<TopicAndPartition,Long> getOffsets(String zkServers,
                                                       String groupID,
                                                       String topic) {
    ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(groupID, topic);
    Map<TopicAndPartition,Long> offsets = new HashMap<>();
    try (AutoZkClient zkClient = new AutoZkClient(zkServers)) {
      List<Object> partitions = JavaConversions.seqAsJavaList(
          ZkUtils.getPartitionsForTopics(
            zkClient,
            JavaConversions.asScalaBuffer(Collections.singletonList(topic))).head()._2());
      for (Object partition : partitions) {
        String partitionOffsetPath = topicDirs.consumerOffsetDir() + "/" + partition;
        Option<String> maybeOffset = ZkUtils.readDataMaybeNull(zkClient, partitionOffsetPath)._1();
        long offset = maybeOffset.isDefined() ? Long.parseLong(maybeOffset.get()) : 0L;
        TopicAndPartition topicAndPartition =
            new TopicAndPartition(topic, Integer.parseInt(partition.toString()));
        offsets.put(topicAndPartition, offset);
      }
    }
    return offsets;
  }

  /**
   * @param zkServers Zookeeper server string: host1:port1[,host2:port2,...]
   * @param groupID consumer group to update
   * @param offsets mapping of (topic and) partition to offset to push to Zookeeper
   */
  public static void setOffsets(String zkServers,
                                String groupID,
                                Map<TopicAndPartition,Long> offsets) {
    try (AutoZkClient zkClient = new AutoZkClient(zkServers)) {
      for (Map.Entry<TopicAndPartition,Long> entry : offsets.entrySet()) {
        TopicAndPartition topicAndPartition = entry.getKey();
        ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(groupID, topicAndPartition.topic());
        int partition = topicAndPartition.partition();
        long offset = entry.getValue();
        String partitionOffsetPath = topicDirs.consumerOffsetDir() + "/" + partition;
        ZkUtils.updatePersistentPath(zkClient, partitionOffsetPath, Long.toString(offset));
      }
    }
  }

  // Just exists for Closeable convenience
  private static final class AutoZkClient extends ZkClient implements Closeable {
    AutoZkClient(String zkServers) {
      super(zkServers, ZK_TIMEOUT_MSEC, ZK_TIMEOUT_MSEC, ZKStringSerializer$.MODULE$);
    }
  }

}
