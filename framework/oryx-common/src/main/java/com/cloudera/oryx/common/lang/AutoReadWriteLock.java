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

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Makes a {@link ReadWriteLock} that can return {@link AutoLock}s and exposes
 * {@link #autoReadLock()} and {@link #autoWriteLock()} like {@link AutoLock#autoLock()}.
 *
 * {@code
 *   ReadWriteLock lock = ...;
 *   ...
 *   AutoReadWriteLock autoLock = new AutoReadWriteLock(lock);
 *   // Not locked
 *   try (AutoLock al = autoLock.autoReadLock()) { // variable required but unused
 *     // Locked
 *     ...
 *   }
 *   // Not locked
 * }
 */
public final class AutoReadWriteLock implements ReadWriteLock {

  private final AutoLock readLock;
  private final AutoLock writeLock;

  /**
   * Manages a new {@link ReentrantReadWriteLock}.
   */
  public AutoReadWriteLock() {
    this(new ReentrantReadWriteLock());
  }

  /**
   * @param lock lock to manage
   */
  public AutoReadWriteLock(ReadWriteLock lock) {
    this.readLock = new AutoLock(lock.readLock());
    this.writeLock = new AutoLock(lock.writeLock());
  }

  @Override
  public AutoLock readLock() {
    return readLock;
  }

  @Override
  public AutoLock writeLock() {
    return writeLock;
  }

  /**
   * @return the {@link ReadWriteLock#readLock()}, locked
   */
  public AutoLock autoReadLock() {
    return readLock.autoLock();
  }

  /**
   * @return the {@link ReadWriteLock#writeLock()} ()}, locked
   */
  public AutoLock autoWriteLock() {
    return writeLock.autoLock();
  }

  @Override
  public String toString() {
    return "AutoReadWriteLock[read:" + readLock + ", write:" + writeLock + "]";
  }

}
