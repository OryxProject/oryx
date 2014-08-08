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

package com.cloudera.oryx.lambda.serving;

import java.io.IOException;
import java.util.Iterator;

import com.cloudera.oryx.common.collection.Pair;

/**
 * Implementations of this interface maintain, in memory, the current state of a model in the
 * serving layer. It is given a reference to a stream of updates during initialization,
 * and consumes models and updates from it, and updates in-memory state accordingly.
 *
 * @param <U> type of update message read/written
 */
public interface ServingModelManager<U> {

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
   * @return a reference to the current state of the model in memory. Note that the model state
   *  may be updated asynchronously from another thread and so access to the data structure must
   *  be treated. May be {@code null} if no model is available.
   */
  ServingModel getModel();

}
