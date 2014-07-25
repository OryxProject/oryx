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

package com.cloudera.oryx.common.io;

import java.io.Closeable;
import java.io.IOException;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simply closes a {@link Closeable}.
 */
public final class ClosingRunnable implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(ClosingRunnable.class);

  private final Closeable closeable;

  public ClosingRunnable(Closeable closeable) {
    Preconditions.checkNotNull(closeable);
    this.closeable = closeable;
  }

  @Override
  public void run() {
    try {
      closeable.close();
    } catch (IOException e) {
      log.warn("Failed to close {}", closeable, e);
    }
  }

}
