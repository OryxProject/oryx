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

package com.cloudera.oryx.api.serving;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;

import com.cloudera.oryx.api.KeyMessage;

/**
 * Implementations of this interface maintain, in memory, the current state of a model in the
 * serving layer. It is given a reference to a stream of updates during initialization,
 * and consumes models and updates from it, and updates in-memory state accordingly.
 *
 * @param <U> type of update message read/written
 * @since 2.0.0
 */
public interface ServingModelManager<U> extends Closeable {

  /**
   * Called by the framework to initiate a continuous process of reading models, and reading
   * from the input topic and updating model state in memory, and issuing updates to the
   * update topic. This will be executed asynchronously and may block. Note that an exception
   * or error thrown from this method is fatal and shuts down processing.
   *
   * @param updateIterator iterator to read models from
   * @param hadoopConf Hadoop context, which may be required for reading from HDFS
   * @throws IOException if an error occurs while reading updates
   * @since 2.0.0
   */
  void consume(Iterator<KeyMessage<String,U>> updateIterator, Configuration hadoopConf) throws IOException;

  /**
   * @return configuration for the serving layer
   * @since 2.0.0
   */
  Config getConfig();

  /**
   * @return in-memory model representation
   * @since 2.0.0
   */
  ServingModel getModel();

  /**
   * @return true iff the model is considered read-only and not updateable
   * @since 2.0.0
   */
  boolean isReadOnly();

  /**
   * @since 2.0.0
   */
  @Override
  default void close() {
    // do nothing
  }

}
