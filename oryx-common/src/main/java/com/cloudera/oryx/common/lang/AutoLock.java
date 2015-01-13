/*
 * Copyright (c) 2015, Cloudera and Intel, Inc. All Rights Reserved.
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

import java.util.concurrent.locks.Lock;

/**
 * Makes a {@link Lock} into an {@link AutoCloseable} for use with try-with-resources:
 *
 * {@code
 *   try (AutoLock al = new AutoLock(lock)) {
 *     ...
 *   }
 * }
 */
public final class AutoLock implements AutoCloseable {

  private final Lock lock;

  /**
   * Locks the given {@link Lock}.
   * @param lock lock to manage
   */
  public AutoLock(Lock lock) {
    this.lock = lock;
    lock.lock();
  }

  /**
   * Unlocks the underlying {@link Lock}.
   */
  @Override
  public void close() {
    lock.unlock();
  }

}
