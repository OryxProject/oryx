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

import java.util.Objects;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Callable} that logs errors thrown from {@link #call()}. Useful in cases where
 * it would otherwise silently disappear into an executor. Static factory methods can create
 * a {@link Callable} from most any lambda expression.
 *
 * @param <V> result type
 */
public final class LoggingCallable<V> implements Callable<V> {

  private static final Logger log = LoggerFactory.getLogger(LoggingCallable.class);

  private final Callable<V> delegate;

  private LoggingCallable(Callable<V> delegate) {
    Objects.requireNonNull(delegate);
    this.delegate = delegate;
  }

  public static <V> LoggingCallable<V> log(Callable<V> delegate) {
    return new LoggingCallable<>(delegate);
  }

  public static LoggingCallable<Void> log(AllowExceptionSupplier delegate) {
    return log(() -> {
      delegate.get();
      return null;
    });
  }

  @Override
  public V call() throws Exception {
    try {
      return delegate.call();
    } catch (Throwable t) {
      log.warn("Unexpected error in {}", delegate, t);
      throw t;
    }
  }

  public Runnable asRunnable() {
    return () -> {
      try {
        delegate.call();
      } catch (Exception e) {
        log.warn("Unexpected error in {}", delegate, e);
        throw new IllegalStateException(e);
      } catch (Throwable t) {
        log.warn("Unexpected error in {}", delegate, t);
        throw t;
      }
    };
  }

  /**
   * Like {@link java.util.function.Supplier} but allows {@link Exception} from
   * {@link java.util.function.Supplier#get()}.
   */
  @FunctionalInterface
  public interface AllowExceptionSupplier {
    void get() throws Exception;
  }

}
