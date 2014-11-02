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

package com.cloudera.oryx.lambda.fn;

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

public final class FunctionsTest extends OryxTest {

  @Test
  public void testSumInt() throws Exception {
    assertEquals(3, Functions.SUM_INT.call(2, 1).intValue());
    assertEquals(2, Functions.SUM_INT.call(2, 0).intValue());
    assertEquals(-3, Functions.SUM_INT.call(-2, -1).intValue());
    assertEquals(Integer.MAX_VALUE, Functions.SUM_INT.call(Integer.MAX_VALUE, 0).intValue());
  }

  @Test
  public void testSumLong() throws Exception {
    assertEquals(3L, Functions.SUM_LONG.call(2L, 1L).longValue());
    assertEquals(2L, Functions.SUM_LONG.call(2L, 0L).longValue());
    assertEquals(-3L, Functions.SUM_LONG.call(-2L, -1L).longValue());
    assertEquals(Long.MAX_VALUE, Functions.SUM_LONG.call(Long.MAX_VALUE, 0L).longValue());
  }

  @Test
  public void testSumDouble() throws Exception {
    assertEquals(3.0, Functions.SUM_DOUBLE.call(2.0, 1.0).doubleValue());
    assertEquals(2.0, Functions.SUM_DOUBLE.call(2.0, 0.0).doubleValue());
    assertEquals(-3.0, Functions.SUM_DOUBLE.call(-2.0, -1.0).doubleValue());
    assertEquals(Double.POSITIVE_INFINITY,
                 Functions.SUM_DOUBLE.call(Double.POSITIVE_INFINITY, 0.0).doubleValue());
    assertEquals(Double.NEGATIVE_INFINITY,
                 Functions.SUM_DOUBLE.call(Double.NEGATIVE_INFINITY, 0.0).doubleValue());
    assertTrue(Double.isNaN(Functions.SUM_DOUBLE.call(1.0, Double.NaN)));
    assertEquals(1.0, Functions.SUM_DOUBLE.call(Double.NaN, 1.0).doubleValue());
  }

  @Test
  public void testToString() throws Exception {
    assertEquals("1.0", Functions.toStringValue().call(1.0));
    assertEquals("null", Functions.toStringValue().call(null));
  }

  @Test
  public void testLast() throws Exception {
    assertEquals(3.0, Functions.last().call(7.0, 3.0));
  }

}
