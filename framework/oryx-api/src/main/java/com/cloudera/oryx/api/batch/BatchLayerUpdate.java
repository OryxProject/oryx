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

package com.cloudera.oryx.api.batch;

import java.io.IOException;
import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.cloudera.oryx.api.TopicProducer;

/**
 * Implementations of this interface define the update process for an instance of
 * the batch layer. It specifies what happens in one batch update cycle. Given
 * the time, and access to both past and current data, it defines some update process
 * in Spark, which produces some output (e.g. a machine learning model in PMML).
 *
 * @param <K> type of key read from input topic
 * @param <M> type of message read from input topic
 * @param <U> type of model message written
 */
public interface BatchLayerUpdate<K,M,U> extends Serializable {

  /**
   * @param sparkContext Spark context
   * @param timestamp timestamp of current interval
   * @param newData data that has arrived in current interval
   * @param pastData all previously-known data (may be {@code null})
   * @param modelDirString String representation of path where models should be output, if desired
   * @param modelUpdateTopic topic to push models onto, if desired
   * @throws IOException if an error occurs during execution of the update function
   * @throws InterruptedException if the caller is interrupted waiting for parallel tasks
   *  to complete
   */
  void runUpdate(JavaSparkContext sparkContext,
                 long timestamp,
                 JavaPairRDD<K,M> newData,
                 JavaPairRDD<K,M> pastData,
                 String modelDirString,
                 TopicProducer<String,U> modelUpdateTopic)
      throws IOException, InterruptedException;

}
