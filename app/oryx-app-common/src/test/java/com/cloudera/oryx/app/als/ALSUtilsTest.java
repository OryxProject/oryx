/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
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

package com.cloudera.oryx.app.als;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.math.LinearSystemSolver;
import com.cloudera.oryx.common.math.Solver;
import com.cloudera.oryx.common.math.VectorMath;

public final class ALSUtilsTest extends OryxTest {

  @Test
  public void testImplicitQui() {
    assertTrue(Double.isNaN(ALSUtils.computeTargetQui(true, 0.0, 1.0)));
    assertTrue(Double.isNaN(ALSUtils.computeTargetQui(true, 0.0, 0.0)));
    assertTrue(Double.isNaN(ALSUtils.computeTargetQui(true, 0.0, -1.0)));
    assertTrue(Double.isNaN(ALSUtils.computeTargetQui(true, 0.5, 1.0)));
    assertTrue(Double.isNaN(ALSUtils.computeTargetQui(true, -0.5, 0.0)));
    assertEquals(0.75, ALSUtils.computeTargetQui(true, 1.0, 0.5));
    assertEquals(0.25, ALSUtils.computeTargetQui(true, -1.0, 0.5));
    for (double d : new double[] { -1.0 , 0.0, 0.5, 1.0, 2.0 }) {
      assertEquals(d, ALSUtils.computeTargetQui(false, d, 0.0));
    }
  }

  // Utilities used in ALS-related tests

  /**
   * @param id nonnegative ID
   * @return string like "A0", "B1", ... "A26" ...
   */
  public static String idToStringID(int id) {
    return Character.toString((char) ('A' + Integer.remainderUnsigned(id, 26))) + Integer.toString(id);
  }

  /**
   * @param stringID string ID like "A0", "B1", etc.
   * @return numeric ID portion 0, 1, etc.
   */
  public static int stringIDtoID(String stringID) {
    return Integer.parseInt(stringID.substring(1));
  }

  @Test
  public void testComputeUpdatedXu() {
    Collection<float[]> rows = Arrays.asList(new float[][]{
        {1.0f, 2.0f},
        {3.0f, 0.0f},
        {0.0f, 1.0f},
    });
    Solver solver = LinearSystemSolver.getSolver(VectorMath.transposeTimesSelf(rows));

    assertNull(ALSUtils.computeUpdatedXu(solver, 1.0, null, null, true));

    assertArrayEquals(new float[] { 0.13043478f, 0.097826086f },
                      ALSUtils.computeUpdatedXu(solver, 1.0, null, new float[] { 2.0f, 1.0f }, true));

    assertArrayEquals(new float[] { 0.11594203f, 0.08695652f },
                      ALSUtils.computeUpdatedXu(solver, 0.5, null, new float[] { 2.0f, 1.0f }, true));

    assertArrayEquals(new float[] { 0.16086957f, 0.14565217f },
                      ALSUtils.computeUpdatedXu(solver, 1.0,
                                                new float[] { 0.1f, 0.1f },
                                                new float[] { 2.0f, 1.0f },
                                                true));
  }

}
