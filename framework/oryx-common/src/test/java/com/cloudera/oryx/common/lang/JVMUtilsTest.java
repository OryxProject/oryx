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

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

public final class JVMUtilsTest extends OryxTest {

  @Test
  public void testShutdownHook() {
    // Can't really test this except to verify that no exception is thrown now or at shutdown
    JVMUtils.closeAtShutdown(() -> {});
  }

  @Test
  public void testRunHook() {
    OryxShutdownHook hook = new OryxShutdownHook();
    AtomicInteger a = new AtomicInteger();
    hook.addCloseable(() -> a.set(3));
    hook.run();
    assertEquals(3, a.get());
  }

  @Test
  public void testUsedMemory() {
    // Reasonable guess
    assertGreaterOrEqual(JVMUtils.getUsedMemory(), 1L << 20);
  }

}
