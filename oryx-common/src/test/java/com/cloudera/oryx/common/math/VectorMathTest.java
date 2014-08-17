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

package com.cloudera.oryx.common.math;

import com.cloudera.oryx.common.OryxTest;
import org.junit.Test;

/**
 * Utility class with simple vector-related operations.
 */
public final class VectorMathTest extends OryxTest {

  @Test
  public void testDot() {
    float[] a = { 1.0f, 0.5f, -3.5f };
    float[] b = { 0.0f, -10.3f, -3.0f };
    assertEquals(5.35, VectorMath.dot(a, b), FLOAT_EPSILON);
  }

  @Test
  public void testSmall() {
    float[] a = { 1.0e-24f };
    assertEquals(1.0e-24 * 1.0e-24, VectorMath.dot(a, a));
  }

  @Test
  public void testBig() {
    float[] a = { 1.0e20f };
    assertEquals((double) 1.0e20f * (double) 1.0e20f, VectorMath.dot(a, a));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMismatchedLength() {
    VectorMath.dot(new float[1], new float[2]);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmpty() {
    VectorMath.dot(new float[1], new float[0]);
  }

}