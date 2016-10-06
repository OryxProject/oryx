/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
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

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.kafka.util.KafkaUtils;

/**
 * Function that reads offset range from latest RDD in a streaming job, and updates
 * Zookeeper/Kafka with the latest offset consumed.
 *
 * @param <T> unused
 */
public final class UpdateOffsetsFn<T> implements VoidFunction<JavaRDD<T>> {

  private static final Logger log = LoggerFactory.getLogger(UpdateOffsetsFn.class);

  private final String group;
  private final String inputTopicLockMaster;

  public UpdateOffsetsFn(String group, String inputTopicLockMaster) {
    this.group = group;
    this.inputTopicLockMaster = inputTopicLockMaster;
  }

  /**
   * @param javaRDD RDD whose underlying RDD must be an instance of {@code HasOffsetRanges},
   *  such as {@code KafkaRDD}
   */
  @Override
  public void call(JavaRDD<T> javaRDD) {
    OffsetRange[] ranges = ((HasOffsetRanges) javaRDD.rdd()).offsetRanges();
    Map<Pair<String,Integer>,Long> newOffsets = new HashMap<>(ranges.length);
    for (OffsetRange range : ranges) {
      newOffsets.put(new Pair<>(range.topic(), range.partition()), range.untilOffset());
    }
    log.info("Updating offsets: {}", newOffsets);
    KafkaUtils.setOffsets(inputTopicLockMaster, group, newOffsets);
  }

}
