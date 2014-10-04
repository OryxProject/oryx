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

import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.cloudera.oryx.common.OryxTest;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.junit.Test;

/**
 * Utility class with simple vector-related operations.
 */
public final class VectorMathTest extends OryxTest {

  private static final float[] VEC1 = { 1.0f, 0.5f, -3.5f };
  private static final float[] VEC2 = { 0.0f, -10.3f, -3.0f };
  private static final double[] VEC1D = { 1.0, 0.5, -3.5 };

  @Test
  public void testDotFF() {
    assertEquals(5.35, VectorMath.dot(VEC1, VEC2), FLOAT_EPSILON);
  }

  @Test
  public void testDotDF() {
    assertEquals(5.35, VectorMath.dot(VEC1D, VEC2), FLOAT_EPSILON);
  }

  @Test
  public void testToFloats() {
    assertArrayEquals(new float[] {1.2f}, VectorMath.toFloats(1.2), FLOAT_EPSILON);
  }

  @Test
  public void testToDoubles() {
    assertArrayEquals(new double[] {1.2}, VectorMath.toDoubles(1.2f), FLOAT_EPSILON);
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
    assertEquals(3.674234614174767, VectorMath.norm(VEC1), FLOAT_EPSILON);
    assertEquals(10.72800074571213, VectorMath.norm(VEC2), FLOAT_EPSILON);
  }

  @Test
  public void testTransposeTimesSelf() {
    IntObjectMap<float[]> a = new IntObjectOpenHashMap<>();
    a.put(-1, new float[] {1.3f, -2.0f, 3.0f});
    a.put(1, new float[] {2.0f, 0.0f, 5.0f});
    a.put(3, new float[] {0.0f, -1.5f, 5.5f});
    RealMatrix ata = VectorMath.transposeTimesSelf(a.values());
    RealMatrix expected = new Array2DRowRealMatrix(new double[][] {
        {5.69, -2.6, 13.9},
        {-2.6, 6.25, -14.25},
        {13.9, -14.25, 64.25}
    });
    for (int row = 0; row < 3; row++) {
      for (int col = 0; col < 3; col++) {
        assertEquals(expected.getEntry(row, col), ata.getEntry(row, col), FLOAT_EPSILON);
      }
    }
  }

}