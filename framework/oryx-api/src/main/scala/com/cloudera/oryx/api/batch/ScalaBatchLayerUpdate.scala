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

package com.cloudera.oryx.api.batch

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import com.cloudera.oryx.api.TopicProducer

/**
 * Scala counterpart to Java BatchLayerUpdate.
 *
 * @tparam K type of key read from input topic
 * @tparam M type of message read from input topic
 * @tparam U type of model message written
 * @since 2.0.0
 */
trait ScalaBatchLayerUpdate[K,M,U] {

  /**
   * @param sparkContext Spark context
   * @param timestamp timestamp of current interval
   * @param newData data that has arrived in current interval
   * @param pastData all previously-known data (may be { @code null})
   * @param modelDirString String representation of path where models should be output, if desired
   * @param modelUpdateTopic topic to push models onto, if desired
   * @since 2.0.0
   */
  def configureUpdate(sparkContext: SparkContext,
                      timestamp: Long,
                      newData: RDD[(K,M)],
                      pastData: RDD[(K,M)],
                      modelDirString: String,
                      modelUpdateTopic: TopicProducer[String,U]): Unit

}
