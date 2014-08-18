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

  private static final float[] VEC1 = { 1.0f, 0.5f, -3.5f };
  private static final float[] VEC2 = { 0.0f, -10.3f, -3.0f };

  @Test
  public void testDot() {
    assertEquals(5.35, VectorMath.dot(VEC1, VEC2), FLOAT_EPSILON);
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

  @Test
  public void testNorm() {
    assertEquals(3.674235, VectorMath.norm(VEC1), FLOAT_EPSILON);
    assertEquals(10.728, VectorMath.norm(VEC2), FLOAT_EPSILON);
  }

  @Test
  public void testNormalize() {
    float[] vec = {-1.0f, 2.5f, 3.0f};
    VectorMath.normalize(vec);
    assertEquals(1.0f, (float) VectorMath.norm(vec));
  }
}