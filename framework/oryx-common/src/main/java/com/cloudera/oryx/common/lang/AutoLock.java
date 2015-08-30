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

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Makes a {@link Lock} into an {@link AutoCloseable} for use with try-with-resources:
 *
 * {@code
 *   Lock lock = ...;
 *   ...
 *   AutoLock autoLock = new AutoLock(lock);
 *   // Not locked
 *   try (AutoLock al = autoLock.autoLock()) { // variable required but unused
 *     // Locked
 *     ...
 *   }
 *   // Not locked
 * }
 */
public final class AutoLock implements AutoCloseable, Lock {

  private final Lock lock;

  /**
   * Manages a new {@link ReentrantLock}.
   */
  public AutoLock() {
    this(new ReentrantLock());
  }

  /**
   * @param lock lock to manage
   */
  public AutoLock(Lock lock) {
    Objects.requireNonNull(lock);
    this.lock = lock;
  }

  /**
   * @return this, after calling {@link #lock()}
   */
  public AutoLock autoLock() {
    lock();
    return this;
  }

  /**
   * Unlocks the underlying {@link Lock}.
   */
  @Override
  public void close() {
    unlock();
  }

  @Override
  public void lock() {
    lock.lock();
  }

  @Override
  public void lockInterruptibly() throws InterruptedException {
    lock.lockInterruptibly();
  }

  @Override
  public boolean tryLock() {
    return lock.tryLock();
  }

  @Override
  public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
    return lock.tryLock(time, unit);
  }

  @Override
  public void unlock() {
    lock.unlock();
  }

  @Override
  public Condition newCondition() {
    return lock.newCondition();
  }

  @Override
  public String toString() {
    return "AutoLock:" + lock;
  }

}
