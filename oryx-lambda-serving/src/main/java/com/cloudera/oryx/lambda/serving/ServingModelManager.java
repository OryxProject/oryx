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

import java.io.Closeable;

import kafka.consumer.ConsumerIterator;

/**
 * Implementations of this interface maintain, in memory, the current state of a model in the
 * serving layer. It is given a reference to a Kafka queue during initialization, and consumes
 * models and updates from it, and updates in-memory state accordingly.
 */
public interface ServingModelManager extends Closeable {

  /**
   * Called by the framework to initiate a continuous process of reading from the queue
   * and updating model state in memory. This will be executed asynchronously and may block.
   *
   * @param updateIterator stream of updates to read
   */
  void start(ConsumerIterator<String,String> updateIterator);

  /**
   * @return a reference to the current state of the model in memory. Note that the model state
   *  may be updated asynchronously from another thread and so access to the data structure must
   *  be treated. May be {@code null} if no model is available.
   */
  ServingModel getModel();

}
