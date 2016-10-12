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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import kafka.admin.AdminUtils;
import kafka.api.OffsetRequest$;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.common.TopicExistsException;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.utils.ZKGroupTopicDirs;
import kafka.utils.ZkUtils;
import kafka.utils.ZkUtils$;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.JavaConversions;

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
        log.info("Creating topic {}", topic);
        try {
          AdminUtils.createTopic(zkUtils, topic, partitions, 1, topicProperties);
          log.info("Created Zookeeper topic {}", topic);
        } catch (TopicExistsException tee) {
          log.info("Zookeeper topic {} already exists", topic);
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
   * @param groupID consumer group to get offsets for
   * @param topic topic to get offsets for
   * @return mapping of (topic and) partition to offset
   */
  public static Map<Pair<String,Integer>,Long> getOffsets(String zkServers,
                                                          String groupID,
                                                          String topic) {
    ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(groupID, topic);
    Map<Pair<String,Integer>,Long> offsets = new HashMap<>();
    ZkUtils zkUtils = ZkUtils.apply(zkServers, ZK_TIMEOUT_MSEC, ZK_TIMEOUT_MSEC, false);
    try {
      List<?> partitions = JavaConversions.seqAsJavaList(
          zkUtils.getPartitionsForTopics(
            JavaConversions.asScalaBuffer(Collections.singletonList(topic))).head()._2());
      partitions.forEach(partition -> {
        String partitionOffsetPath = topicDirs.consumerOffsetDir() + "/" + partition;
        Option<String> maybeOffset = zkUtils.readDataMaybeNull(partitionOffsetPath)._1();
        Long offset = maybeOffset.isDefined() ? Long.parseLong(maybeOffset.get()) : null;
        offsets.put(new Pair<>(topic, Integer.parseInt(partition.toString())), offset);
      });
    } finally {
      zkUtils.close();
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
                                Map<Pair<String,Integer>,Long> offsets) {
    ZkUtils zkUtils = ZkUtils.apply(zkServers, ZK_TIMEOUT_MSEC, ZK_TIMEOUT_MSEC, false);
    try {
      offsets.forEach((topicAndPartition, offset) -> {
        ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(groupID, topicAndPartition.getFirst());
        int partition = topicAndPartition.getSecond();
        String partitionOffsetPath = topicDirs.consumerOffsetDir() + "/" + partition;
        zkUtils.updatePersistentPath(partitionOffsetPath,
                                     Long.toString(offset),
                                     ZkUtils$.MODULE$.DefaultAcls(false));
      });
    } finally {
      zkUtils.close();
    }
  }

  // Inspired by KafkaCluster from Spark Kafka 0.8 connector:

  public static void fillInLatestOffsets(Map<Pair<String,Integer>,Long> offsets,
                                         Map<String,String> kafkaParams) {

    Properties props = new Properties();
    kafkaParams.forEach(props::put);
    ConsumerConfig config = new ConsumerConfig(props);

    Map<TopicAndPartition,PartitionOffsetRequestInfo> latestRequests = new HashMap<>();
    Map<TopicAndPartition,PartitionOffsetRequestInfo> earliestRequests = new HashMap<>();
    offsets.keySet().forEach(topicPartition -> {
      TopicAndPartition tAndP = new TopicAndPartition(topicPartition.getFirst(), topicPartition.getSecond());
      latestRequests.put(tAndP, new PartitionOffsetRequestInfo(OffsetRequest$.MODULE$.LatestTime(), 1));
      earliestRequests.put(tAndP, new PartitionOffsetRequestInfo(OffsetRequest$.MODULE$.EarliestTime(), 1));
    });
    OffsetRequest latestRequest = new OffsetRequest(latestRequests,
                                                    OffsetRequest$.MODULE$.CurrentVersion(),
                                                    config.clientId());
    OffsetRequest earliestRequest = new OffsetRequest(earliestRequests,
                                                      OffsetRequest$.MODULE$.CurrentVersion(),
                                                      config.clientId());

    SimpleConsumer consumer = null;
    for (String hostPort : kafkaParams.get("bootstrap.servers").split(",")) {
      log.info("Connecting to broker {}", hostPort);
      String[] hp = hostPort.split(":");
      String host = hp[0];
      int port = Integer.parseInt(hp[1]);
      try {
        consumer = new SimpleConsumer(host, port,
                                      config.socketTimeoutMs(),
                                      config.socketReceiveBufferBytes(),
                                      config.clientId());
        break;
      } catch (Exception e) {
        log.warn("Error while connecting to broker {}:{}", host, port, e);
      }
    }
    Objects.requireNonNull(consumer, "No available brokers");

    try {
      OffsetResponse latestResponse = requestOffsets(consumer, latestRequest);
      OffsetResponse earliestResponse = requestOffsets(consumer, earliestRequest);
      offsets.keySet().forEach(topicPartition -> {
        long latestTopicOffset = getOffset(latestResponse, topicPartition);
        Long currentOffset = offsets.get(topicPartition);
        if (currentOffset == null) {
          log.info("No initial offsets for {}; using latest offset {} from topic",
                   topicPartition, latestTopicOffset);
          offsets.put(topicPartition, latestTopicOffset);
        } else if (currentOffset > latestTopicOffset) {
          log.warn("Initial offset {} for {} after latest offset {} from topic! using topic offset",
                   currentOffset, topicPartition, latestTopicOffset);
          offsets.put(topicPartition, latestTopicOffset);
        } else {
          long earliestTopicOffset = getOffset(earliestResponse, topicPartition);
          if (currentOffset < earliestTopicOffset) {
            log.warn("Initial offset {} for {} before earliest offset {} from topic! using topic offset",
                     currentOffset, topicPartition, earliestTopicOffset);
            offsets.put(topicPartition, earliestTopicOffset);
          }
        }
      });
    } finally {
      consumer.close();
    }

  }

  private static long getOffset(OffsetResponse response, Pair<String,Integer> topicPartition) {
    String topic = topicPartition.getFirst();
    int partition = topicPartition.getSecond();
    long[] offsets = response.offsets(topic, partition);
    if (offsets.length > 0) {
      return offsets[0];
    }
    short errorCode = response.errorCode(topic, partition);
    if (errorCode == ErrorMapping.UnknownTopicOrPartitionCode()) {
      return 0;
    }
    throw new IllegalStateException(
        "Error reading offset for " + topic + " / " + partition + ": " +
        ErrorMapping.exceptionNameFor(errorCode));
  }

  private static OffsetResponse requestOffsets(SimpleConsumer consumer, OffsetRequest request) {
    return consumer.getOffsetsBefore(request);
  }

}
