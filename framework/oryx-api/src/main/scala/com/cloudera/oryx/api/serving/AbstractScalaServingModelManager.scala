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

import com.typesafe.config.Config

/**
 * Convenience implementation of [[ScalaServingModelManager]] that provides several default implementations.
 *
 * @param config  Oryx [[Config]] object
 * @tparam U type of update message read/written
 * @since 2.0.0
 */
abstract class AbstractScalaServingModelManager[U](private val config: Config) extends ScalaServingModelManager[U] {

  private val readOnly = config.getBoolean("oryx.serving.api.read-only")

  /**
   * @since 2.0.0
   */
  override def getConfig = config

  /**
   * @since 2.0.0
   */
  override def isReadOnly = readOnly

  /**
   * @since 2.0.0
   */
  override def close(): Unit = {}

}
