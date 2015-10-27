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

package com.cloudera.oryx.api.serving

import com.cloudera.oryx.api.KeyMessage
import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration

/**
 * Scala counterpart to Java ServingModelManager.
 *
 * @tparam U type of update message read/written
 * @since 2.0.0
 */
trait ScalaServingModelManager[U] {

  /**
   * Called by the framework to initiate a continuous process of reading models, and reading
   * from the input topic and updating model state in memory, and issuing updates to the
   * update topic. This will be executed asynchronously and may block.
   *
   * @param updateIterator iterator to read models from
   * @param hadoopConf Hadoop context, which may be required for reading from HDFS
   * @since 2.0.0
   */
  def consume(updateIterator: Iterator[KeyMessage[String,U]], hadoopConf: Configuration): Unit

  /**
   * @return configuration for the serving layer
   * @since 2.0.0
   */
  def getConfig: Config

  /**
   * @return in-memory model representation
   * @since 2.0.0
   */
  def getModel: ServingModel

  /**
   * @return true iff the model is considered read-only and not updateable
   * @since 2.0.0
   */
  def isReadOnly: Boolean

  /**
   * @since 2.0.0
   */
  def close(): Unit

}
