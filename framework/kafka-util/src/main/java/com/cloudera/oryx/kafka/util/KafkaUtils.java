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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.api.OffsetRequest$;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
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
          AdminUtils.createTopic(
              zkUtils, topic, partitions, 1, topicProperties, RackAwareMode.Enforced$.MODULE$);
          log.info("Created Zookeeper topic {}", topic);
        } catch (RuntimeException re) {
          // Hack to deal with the fact that kafka.common.TopicExistsException became
          // org.apache.kafka.common.errors.TopicExistsException from 0.10.0 to 0.10.1
          if ("TopicExistsException".equals(re.getClass().getSimpleName())) {
            log.info("Zookeeper topic {} already exists", topic);
          } else {
            throw re;
          }
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
        Long offset = maybeOffset.isDefined() ? Long.valueOf(maybeOffset.get()) : null;
        offsets.put(new Pair<>(topic, Integer.valueOf(partition.toString())), offset);
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
  @SuppressWarnings("deprecation")
  public static void setOffsets(String zkServers,
                                String groupID,
                                Map<Pair<String,Integer>,Long> offsets) {
    ZkUtils zkUtils = ZkUtils.apply(zkServers, ZK_TIMEOUT_MSEC, ZK_TIMEOUT_MSEC, false);
    try {
      offsets.forEach((topicAndPartition, offset) -> {
        ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(groupID, topicAndPartition.getFirst());
        int partition = topicAndPartition.getSecond();
        String partitionOffsetPath = topicDirs.consumerOffsetDir() + "/" + partition;
        // TODO replace call below with defaultAcls(false, "") when < 0.10.2 is supported
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

    Map<Pair<String, Integer>, List<TopicAndPartition>> leaderHostPortToTopicPartition =
        getLeadersForTopicPartitions(offsets, kafkaParams, config);
    
    for (Map.Entry<Pair<String,Integer>,List<TopicAndPartition>> entry : leaderHostPortToTopicPartition.entrySet()) {
      // Connect to leader
      String leaderHost = entry.getKey().getFirst();
      int leaderPort = entry.getKey().getSecond();
      log.info("Connecting to broker {}:{}", leaderHost, leaderPort);
      SimpleConsumer consumer = new SimpleConsumer(
          leaderHost, leaderPort,
          config.socketTimeoutMs(),
          config.socketReceiveBufferBytes(),
          config.clientId());

      try {
        List<TopicAndPartition> topicAndPartitions = entry.getValue();

        // Construct request for latest topic/offsets for each that this leader knows about.
        Map<TopicAndPartition,PartitionOffsetRequestInfo> latestRequests = new HashMap<>();
        Map<TopicAndPartition,PartitionOffsetRequestInfo> earliestRequests = new HashMap<>();
        topicAndPartitions.forEach(tAndP -> {
          latestRequests.put(tAndP, new PartitionOffsetRequestInfo(OffsetRequest$.MODULE$.LatestTime(), 1));
          earliestRequests.put(tAndP, new PartitionOffsetRequestInfo(OffsetRequest$.MODULE$.EarliestTime(), 1));
        });
        OffsetRequest latestRequest = new OffsetRequest(latestRequests,
            OffsetRequest$.MODULE$.CurrentVersion(),
            config.clientId());
        OffsetRequest earliestRequest = new OffsetRequest(earliestRequests,
            OffsetRequest$.MODULE$.CurrentVersion(),
            config.clientId());
        
        OffsetResponse latestResponse = requestOffsets(consumer, latestRequest);
        OffsetResponse earliestResponse = requestOffsets(consumer, earliestRequest);

        // For each topic/partition update, parse and use the values
        topicAndPartitions.forEach(topicPartition -> {
          Long latestTopicOffset = getOffset(latestResponse, topicPartition);
          Pair<String,Integer> topicPartitionKey = new Pair<>(topicPartition.topic(), topicPartition.partition());
          Long currentOffset = offsets.get(topicPartitionKey);
          if (currentOffset == null) {
            if (latestTopicOffset == null) {
              log.info("No initial offset for {}, no latest offset from topic; ignoring");
            } else {
              log.info("No initial offset for {}; using latest offset {} from topic",
                  topicPartition, latestTopicOffset);
              offsets.put(topicPartitionKey, latestTopicOffset);
            }
          } else if (latestTopicOffset != null && currentOffset > latestTopicOffset) {
            log.warn("Initial offset {} for {} after latest offset {} from topic! using topic offset",
                currentOffset, topicPartition, latestTopicOffset);
            log.warn("Are you using a stale or incorrect oryx.id?");
            offsets.put(topicPartitionKey, latestTopicOffset);
          } else {
            Long earliestTopicOffset = getOffset(earliestResponse, topicPartition);
            if (earliestTopicOffset != null && currentOffset < earliestTopicOffset) {
              log.warn("Initial offset {} for {} before earliest offset {} from topic! using topic offset",
                  currentOffset, topicPartition, earliestTopicOffset);
              log.warn("Are you using a stale or incorrect oryx.id?");
              offsets.put(topicPartitionKey, earliestTopicOffset);
            }
          }
        });

      } finally {
        consumer.close();
      }
    }

    
    offsets.values().removeIf(Objects::isNull);
  }

  private static Map<Pair<String,Integer>,List<TopicAndPartition>> getLeadersForTopicPartitions(
      Map<Pair<String,Integer>,Long> offsets, Map<String,String> kafkaParams, ConsumerConfig config) {

    // Connect to any leader
    SimpleConsumer consumer = null;
    for (String hostPort : kafkaParams.get("bootstrap.servers").split(",")) {
      log.info("Connecting to broker {}", hostPort);
      String[] hp = hostPort.split(":");
      String host = hp[0];
      int port = Integer.parseInt(hp[1]);
      try {
        consumer = new SimpleConsumer(
            host, port,
            config.socketTimeoutMs(),
            config.socketReceiveBufferBytes(),
            config.clientId());
        break;
      } catch (Exception e) {
        log.warn("Error while connecting to broker {}:{}", host, port, e);
      }
    }
    Objects.requireNonNull(consumer, "No available brokers");

    // Construct a map from broker (host, port) pairs, to the (topic, partition) pairs for which the broker
    // is a leader
    Map<Pair<String,Integer>,List<TopicAndPartition>> leaderHostPortToTopicPartition = new HashMap<>();
    try {
      List<String> allTopics = offsets.keySet().stream().map(Pair::getFirst).collect(Collectors.toList());
      TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(
          OffsetRequest$.MODULE$.CurrentVersion(),
          0,
          config.clientId(),
          allTopics);
      TopicMetadataResponse topicMetadataResponse = consumer.send(topicMetadataRequest);
      for (TopicMetadata topicsMetadata : topicMetadataResponse.topicsMetadata()) {
        String topic = topicsMetadata.topic();
        for (PartitionMetadata partitionMetadata : topicsMetadata.partitionsMetadata()) {
          int partition = partitionMetadata.partitionId();
          String host = partitionMetadata.leader().host();
          int port = partitionMetadata.leader().port();
          leaderHostPortToTopicPartition.computeIfAbsent(new Pair<>(host, port), k -> new ArrayList<>())
              .add(new TopicAndPartition(topic, partition));
        }
      }
    } finally {
      consumer.close();
    }

    return leaderHostPortToTopicPartition;
  }

  private static Long getOffset(OffsetResponse response, TopicAndPartition topicPartition) {
    String topic = topicPartition.topic();
    int partition = topicPartition.partition();
    long[] offsets = response.offsets(topic, partition);
    if (offsets.length > 0) {
      return offsets[0];
    }
    short errorCode = response.errorCode(topic, partition);
    if (errorCode == ErrorMapping.UnknownTopicOrPartitionCode()) {
      log.info("Unknown topic or partition {} {}", topic, partition);
      return null;
    }
    throw new IllegalStateException(
        "Error reading offset for " + topic + " / " + partition + ": " +
        ErrorMapping.exceptionNameFor(errorCode));
  }

  private static OffsetResponse requestOffsets(SimpleConsumer consumer, OffsetRequest request) {
    return consumer.getOffsetsBefore(request);
  }

}
