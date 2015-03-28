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

package com.cloudera.oryx.lambda.speed;

import java.io.IOException;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.api.speed.SpeedModelManager;
import com.cloudera.oryx.api.TopicProducer;
import com.cloudera.oryx.lambda.TopicProducerImpl;

/**
 * Main Spark Streaming function for the speed layer that collects and publishes update to
 * a Kafka topic.
 */
public final class SpeedLayerUpdate<K,M,U> implements Function<JavaPairRDD<K,M>,Void> {

  private static final Logger log = LoggerFactory.getLogger(SpeedLayerUpdate.class);

  private final SpeedModelManager<K,M,U> modelManager;
  private final String updateBroker;
  private final String updateTopic;

  public SpeedLayerUpdate(SpeedModelManager<K,M,U> modelManager,
                          String updateBroker,
                          String updateTopic) {
    this.modelManager = modelManager;
    this.updateBroker = updateBroker;
    this.updateTopic = updateTopic;
  }

  @Override
  public Void call(JavaPairRDD<K,M> newData) throws IOException {
    if (newData.isEmpty()) {
      log.debug("RDD was empty");
    } else {
      try (TopicProducer<String,U> producer = new TopicProducerImpl<>(updateBroker, updateTopic)) {
        for (U update : modelManager.buildUpdates(newData)) {
          producer.send("UP", update);
        }
      }
    }
    return null;
  }

}
