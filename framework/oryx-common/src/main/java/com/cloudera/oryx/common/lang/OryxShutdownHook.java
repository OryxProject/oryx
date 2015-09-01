/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
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

import java.io.Closeable;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Objects;

import com.google.common.base.Preconditions;

import com.cloudera.oryx.common.io.IOUtils;

/**
 * Intended for use with {@link Runtime#addShutdownHook(Thread)} or similar mechanisms, this is a
 * {@link Runnable} that is configured with a list of {@link Closeable}s that are to be closed
 * at shutdown, when its {@link #run()} is called.
 */
public final class OryxShutdownHook implements Runnable {

  private final Deque<Closeable> closeAtShutdown = new LinkedList<>();
  private volatile boolean triggered;

  @Override
  public void run() {
    triggered = true;
    synchronized (closeAtShutdown) {
      closeAtShutdown.forEach(IOUtils::closeQuietly);
    }
  }

  /**
   * @param closeable object to close at shutdown
   * @return {@code true} iff this is the first object to be registered
   * @throws IllegalStateException if already shutting down
   */
  public boolean addCloseable(Closeable closeable) {
    Objects.requireNonNull(closeable);
    Preconditions.checkState(!triggered, "Can't add closeable %s; already shutting down", closeable);
    synchronized (closeAtShutdown) {
      boolean wasFirst = closeAtShutdown.isEmpty();
      closeAtShutdown.push(closeable);
      return wasFirst;
    }
  }

}
