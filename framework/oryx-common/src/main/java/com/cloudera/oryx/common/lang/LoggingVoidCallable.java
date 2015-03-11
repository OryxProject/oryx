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

package com.cloudera.oryx.common.lang;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Callable} that logs errors thrown from {@link #call()}. Useful in cases where
 * it would otherwise silently disappear into an executor.
 */
public abstract class LoggingVoidCallable implements Callable<Void> {

  private static final Logger log = LoggerFactory.getLogger(LoggingVoidCallable.class);

  @Override
  public final Void call() {
    try {
      doCall();
      return null;
    } catch (Exception e) {
      log.warn("Unexpected error in {}", this, e);
      throw new IllegalStateException(e);
    } catch (Throwable t) {
      log.warn("Unexpected error in {}", this, t);
      throw t;
    }
  }

  public abstract void doCall() throws Exception;

}
