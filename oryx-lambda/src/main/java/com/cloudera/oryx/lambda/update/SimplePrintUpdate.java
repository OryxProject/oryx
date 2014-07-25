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

package com.cloudera.oryx.lambda.update;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import com.cloudera.oryx.lambda.QueueProducer;

/**
 * A simple {@link BatchLayerUpdate} which logs all data received.
 * May be useful for testing or debugging.
 */
public final class SimplePrintUpdate<K,M,O> implements BatchLayerUpdate<K,M,O> {

  private static final Logger log = LoggerFactory.getLogger(SimplePrintUpdate.class);

  @Override
  public void configureUpdate(JavaSparkContext sparkContext,
                              long timestamp,
                              JavaPairRDD<K,M> newData,
                              JavaPairRDD<K,M> pastData,
                              String modelDirString,
                              QueueProducer<String,O> modelUpdateQueue) {
    JavaPairRDD<K,M> allData = pastData == null ? newData : newData.union(pastData);
    allData.foreach(new PrintFunction<K,M>());
  }

  private static final class PrintFunction<K,M> implements VoidFunction<Tuple2<K,M>> {
    @Override
    public void call(Tuple2<K,M> keyMessage) {
      log.info("{} = {}", keyMessage._1(), keyMessage._2());
    }
  }

}
