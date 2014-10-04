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

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

public final class ClosingRunnableTest extends OryxTest {

  @Test
  public void testClose() {
    final int[] holder = new int[1];
    Closeable closeable = new Closeable() {
      @Override
      public void close() {
        holder[0]++;
      }
    };
    new ClosingRunnable(closeable).run();
    assertEquals(1, holder[0]);
  }

}
