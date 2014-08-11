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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;

import com.cloudera.oryx.common.collection.Pair;

/**
 * Implementations of this interface maintain, in memory, the current state of a model in the
 * speed layer. It is given a reference to stream of updates during initialization, and consumes
 * models and new input, updates in-memory state, and produces updates accordingly.
 *
 * @param <K> type of key read from input queue
 * @param <M> type of message read from input queue
 * @param <U> type of update message read/written
 */
public interface SpeedModelManager<K,M,U> extends Closeable {

  /**
   * Called by the framework to initiate a continuous process of reading models, and reading
   * from the input queue and updating model state in memory, and issuing updates to the
   * update queue. This will be executed asynchronously and may block.
   *
   * @param updateIterator iterator to read models from
   * @throws IOException if an error occurs while reading updates
   */
  void consume(Iterator<Pair<String,U>> updateIterator) throws IOException;

  /**
   * @param newData RDD of raw new data from the queue
   * @return updates to publish on the update queue
   */
  Collection<U> buildUpdates(JavaPairRDD<K,M> newData);

  @Override
  void close();

}
