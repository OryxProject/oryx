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

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import com.cloudera.oryx.lambda.update.BatchLayerUpdate;

/**
 * A dummy {@link com.cloudera.oryx.lambda.update.BatchLayerUpdate} that collects data seen by the
 * framework in a given {@link List}. Assists testing.
 */
public final class MockUpdate implements BatchLayerUpdate<String,String,String> {

  private static List<IntervalData<String,String>> holder;

  static void setIntervalDataHolder(List<IntervalData<String,String>> holder) {
    MockUpdate.holder = holder;
  }

  @Override
  public void configureUpdate(JavaSparkContext sparkContext,
                              long timestamp,
                              JavaPairRDD<String,String> newData,
                              JavaPairRDD<String,String> pastData,
                              String modelDirString,
                              QueueProducer<String,String> modelUpdateQueue) {
    holder.add(new IntervalData<>(timestamp, collect(newData), collect(pastData)));
  }

  private static Collection<Tuple2<String,String>> collect(JavaPairRDD<String,String> rdd) {
    if (rdd == null) {
      return Collections.emptyList();
    } else {
      return rdd.collect();
    }
  }

}
