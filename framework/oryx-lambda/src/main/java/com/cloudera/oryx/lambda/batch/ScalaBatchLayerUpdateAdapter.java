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

package com.cloudera.oryx.lambda.batch;

import com.google.common.base.Preconditions;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.cloudera.oryx.api.TopicProducer;
import com.cloudera.oryx.api.batch.BatchLayerUpdate;
import com.cloudera.oryx.api.batch.ScalaBatchLayerUpdate;

/**
 * Adapts a {@link ScalaBatchLayerUpdate} to be a Java {@link BatchLayerUpdate}.
 *
 * @param <K> type of key read from input topic
 * @param <M> type of message read from input topic
 * @param <U> type of model message written
 */
public final class ScalaBatchLayerUpdateAdapter<K,M,U> implements BatchLayerUpdate<K,M,U> {

  private final ScalaBatchLayerUpdate<K,M,U> scalaUpdate;

  public ScalaBatchLayerUpdateAdapter(ScalaBatchLayerUpdate<K,M,U> scalaUpdate) {
    Preconditions.checkNotNull(scalaUpdate);
    this.scalaUpdate = scalaUpdate;
  }

  @Override
  public void runUpdate(JavaSparkContext sparkContext,
                        long timestamp,
                        JavaPairRDD<K,M> newData,
                        JavaPairRDD<K,M> pastData,
                        String modelDirString,
                        TopicProducer<String,U> modelUpdateTopic) {
    scalaUpdate.configureUpdate(sparkContext.sc(),
                                timestamp,
                                newData.rdd(),
                                pastData.rdd(),
                                modelDirString,
                                modelUpdateTopic);
  }

}
