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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.random.RandomManager;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.random.RandomGenerator;
import org.junit.Test;

/**
 * Utility class with simple vector-related operations.
 */
public final class VectorMathTest extends OryxTest {

  private static final float[] VEC1 = { 1.0f, 0.5f, -3.5f };
  private static final float[] VEC2 = { 0.0f, -10.3f, -3.0f };

  @Test
  public void testDotFF() {
    assertEquals(5.35, VectorMath.dot(VEC1, VEC2), FLOAT_EPSILON);
  }

  @Test
  public void testParseVector() {
    assertArrayEquals(
        new double[] {-1.0, 2.01, 3.5},
        VectorMath.parseVector(new String[] {"-1.0", "2.01", "3.5"}));
  }

  @Test
  public void testSmall() {
    float[] a = { 1.0e-24f };
    assertEquals(1.0e-24 * 1.0e-24, VectorMath.dot(a, a));
  }

  @Test
  public void testNormF() {
    assertEquals(0.0, VectorMath.norm(new float[] {0.0f}), FLOAT_EPSILON);
    assertEquals(3.674234614174767, VectorMath.norm(VEC1), FLOAT_EPSILON);
    assertEquals(10.72800074571213, VectorMath.norm(VEC2), FLOAT_EPSILON);
  }

  @Test
  public void testTransposeTimesSelf() {
    Map<Integer,float[]> a = new HashMap<>();
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

  @Test
  public void testNullTranspose() {
    assertNull(VectorMath.transposeTimesSelf(null));
    assertNull(VectorMath.transposeTimesSelf(Collections.<float[]>emptyList()));
  }

  @Test
  public void testRandomF() {
    RandomGenerator random = RandomManager.getRandom();
    float[] vec1 = VectorMath.randomVectorF(10, random);
    float[] vec2 = VectorMath.randomVectorF(10, random);
    assertEquals(10, vec1.length);
    assertEquals(10, vec2.length);
    assertFalse(Arrays.equals(vec1, vec2));
  }

}