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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.lambda.QueueProducer;

/**
 * A simple {@link BatchLayerUpdate} which does nothing. May be useful for testing.
 */
public final class NoopUpdate<K,M,O> implements BatchLayerUpdate<K,M,O> {

  private static final Logger log = LoggerFactory.getLogger(NoopUpdate.class);

  @Override
  public void configureUpdate(JavaSparkContext sparkContext,
                              long timestamp,
                              JavaPairRDD<K,M> newData,
                              JavaPairRDD<K,M> pastData,
                              String modelDirString,
                              QueueProducer<String,O> modelUpdateQueue) {
    log.info("configureUpdate at {}", timestamp);
  }

}
