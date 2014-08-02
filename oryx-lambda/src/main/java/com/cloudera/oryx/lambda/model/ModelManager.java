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

package com.cloudera.oryx.lambda.model;

import java.io.Closeable;

import kafka.consumer.ConsumerIterator;
import kafka.javaapi.producer.Producer;

/**
 * Implementations of this interface maintain, in memory, the current state of a model in the
 * speed layer. It is given a reference to a Kafka queue during initialization, and consumes
 * models and new input, updates in-memory state, and produces updates accordingly.
 *
 * @param <K> input key type
 * @param <M> input message type
 */
public interface ModelManager<K,M> extends Closeable {

  /**
   * Called by the framework to initiate a continuous process of reading models, and reading
   * from the input queue and updating model state in memory, and issuing updates to the
   * update queue. This will be executed asynchronously and may block.
   *
   * @param inputIterator stream of input to read
   * @param updateIterator queue to read models from
   * @param updateProducer queue to write updates to
   */
  void start(ConsumerIterator<K,M> inputIterator,
             ConsumerIterator<String,String> updateIterator,
             Producer<String,String> updateProducer);

}
