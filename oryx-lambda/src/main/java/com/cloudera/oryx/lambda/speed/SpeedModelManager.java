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

import com.cloudera.oryx.lambda.QueueProducer;

/**
 * Implementations of this interface maintain, in memory, the current state of a model in the
 * speed layer. It is given a reference to stream of updates during initialization, and consumes
 * models and new input, updates in-memory state, and produces updates accordingly.
 *
 * @param <M> input message type
 */
public interface SpeedModelManager<M> extends Closeable {

  /**
   * Called by the framework to initiate a continuous process of reading models, and reading
   * from the input queue and updating model state in memory, and issuing updates to the
   * update queue. This will be executed asynchronously and may block.
   *
   * @param updateIterator queue to read models from. Values are arrays of 2 stings, the
   *  key and message
   */
  void start(Iterator<String[]> updateIterator) throws IOException;

  /**
   * @param input small batch of recent input
   * @param updateProducer queue to write updates to
   */
  void onInput(Collection<M> input, QueueProducer<String,String> updateProducer);

  /**
   * @return reference to current model in memory
   */
  SpeedModel getModel();

}
