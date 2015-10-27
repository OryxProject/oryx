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

import com.typesafe.config.Config;

/**
 * Convenience implementation of {@link ServingModelManager} that provides several default implementations.
 *
 * @param <U> type of update message read/written
 * @since 2.0.0
 */
public abstract class AbstractServingModelManager<U> implements ServingModelManager<U> {

  private final Config config;
  private final boolean readOnly;

  /**
   * @param config Oryx {@link Config} object
   * @since 2.0.0
   */
  protected AbstractServingModelManager(Config config) {
    this.config = config;
    this.readOnly = config.getBoolean("oryx.serving.api.read-only");
  }

  @Override
  public Config getConfig() {
    return config;
  }

  @Override
  public boolean isReadOnly() {
    return readOnly;
  }

  @Override
  public void close() {
    // do nothing
  }

}
