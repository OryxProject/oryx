/*
 * Copyright (c) 2014, Cloudera, Inc. and Intel Corp. All Rights Reserved.
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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

public final class LoggingTest extends OryxTest {

  private static final StackTraceElement[] NO_STACK = new StackTraceElement[0];
  public static final IOException DUMMY_EXCEPTION = new IOException("It's safe to ignore this exception");
  private static final AssertionError DUMMY_ERROR = new AssertionError("It's safe to ignore this exception");
  static {
    // to not pollute the logs
    DUMMY_EXCEPTION.setStackTrace(NO_STACK);
    DUMMY_ERROR.setStackTrace(NO_STACK);
  }

  @Test
  public void testLoggingCallable() throws Exception {
    Integer result = LoggingCallable.log(() -> 3).call();
    assertEquals(3, result.intValue());
  }

  @Test
  public void testAsRunnable() {
    AtomicInteger a = new AtomicInteger();
    LoggingCallable.log(() -> a.set(3)).asRunnable().run();
    assertEquals(3, a.get());
  }

  @Test(expected = IOException.class)
  public void testLoggingCallableException() throws Exception {
    LoggingCallable.log(() -> { throw DUMMY_EXCEPTION; }).call();
  }

  @Test(expected = AssertionError.class)
  public void testLoggingCallableException2() throws Exception {
    LoggingCallable.log(() -> { throw DUMMY_ERROR; }).call();
  }

}
