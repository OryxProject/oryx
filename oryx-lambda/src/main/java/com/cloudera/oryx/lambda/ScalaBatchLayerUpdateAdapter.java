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

package com.cloudera.oryx.lambda;

import com.google.common.base.Preconditions;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

public final class ScalaBatchLayerUpdateAdapter<K,M,U> implements BatchLayerUpdate<K,M,U> {

  private final ScalaBatchLayerUpdate<K,M,U> scalaUpdate;

  public ScalaBatchLayerUpdateAdapter(ScalaBatchLayerUpdate<K,M,U> scalaUpdate) {
    Preconditions.checkNotNull(scalaUpdate);
    this.scalaUpdate = scalaUpdate;
  }

  @Override
  public void configureUpdate(JavaSparkContext sparkContext,
                              long timestamp,
                              JavaPairRDD<K,M> newData,
                              JavaPairRDD<K,M> pastData,
                              String modelDirString,
                              QueueProducer<String,U> modelUpdateQueue) {
    scalaUpdate.configureUpdate(sparkContext.sc(),
                                timestamp,
                                newData.rdd(),
                                pastData.rdd(),
                                modelDirString,
                                modelUpdateQueue);
  }

}
