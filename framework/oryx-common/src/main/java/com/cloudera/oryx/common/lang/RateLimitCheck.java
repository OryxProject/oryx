/*
 * Copyright (c) 2016, Cloudera, Inc. All Rights Reserved.
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

import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

/**
 * A utility that exposes a {@link #test()} method which returns {@code true} first, and then
 * return {@code false} for all subsequent invocations within the given time interval afterwards.
 * The next call then returns {@code true} and so on. This is useful as a check for rate-limiting
 * some other process.
 */
public final class RateLimitCheck {

  private final long intervalNanos;
  private long nextSuccess;

  public RateLimitCheck(long time, TimeUnit unit) {
    Preconditions.checkArgument(time > 0);
    intervalNanos = TimeUnit.NANOSECONDS.convert(time, unit);
    nextSuccess = System.nanoTime();
  }

  public boolean test() {
    boolean test = false;
    long now = System.nanoTime();
    synchronized (this) {
      if (now >= nextSuccess) {
        test = true;
        nextSuccess = now + intervalNanos;
      }
    }
    return test;
  }

}
