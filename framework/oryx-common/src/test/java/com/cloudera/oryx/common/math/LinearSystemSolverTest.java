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

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

public final class LinearSystemSolverTest extends OryxTest {

  @Test
  public void testNull() {
    assertNull(new LinearSystemSolver().getSolver(null));
  }

  @Test
  public void testSolveFToD() {
    RealMatrix a = new Array2DRowRealMatrix(new double[][] {
        {1.3, -2.0, 3.0},
        {2.0, 0.0, 5.0},
        {0.0, -1.5, 5.5},
    });
    Solver solver = new LinearSystemSolver().getSolver(a);
    assertNotNull(solver);
    double[] y = solver.solveFToD(new float[] {1.0f, 2.0f, 6.5f});
    assertArrayEquals(
        new double[] {-1.9560439560439564,0.002197802197802894,1.1824175824175824}, y);
  }

  @Test
  public void testSolveDToD() {
    RealMatrix a = new Array2DRowRealMatrix(new double[][] {
        {1.3, -2.0, 3.0},
        {2.0, 0.0, 5.0},
        {0.0, -1.5, 5.5},
    });
    Solver solver = new LinearSystemSolver().getSolver(a);
    assertNotNull(solver);
    double[] y = solver.solveDToD(new double[]{1.0, 2.0, 6.5});
    assertArrayEquals(
        new double[] {-1.9560439560439564,0.002197802197802894,1.1824175824175824}, y);
  }

  @Test
  public void testIsNonSingular() {
    RealMatrix nonSingular = new Array2DRowRealMatrix(new double[][] {
        {1.3, -2.0, 3.0},
        {2.0, 0.0, 5.0},
        {0.0, -1.5, 5.5},
    });
    assertTrue(new LinearSystemSolver().isNonSingular(nonSingular));
    RealMatrix singular = new Array2DRowRealMatrix(new double[][] {
        {1.3, -2.0, 3.0},
        {2.6, -4.0, 6.0},
        {0.0, -1.5, 5.5},
    });
    assertFalse(new LinearSystemSolver().isNonSingular(singular));
  }

  @Test
  public void testApparentRank() {
    RealMatrix nearSingular = new Array2DRowRealMatrix(new double[][] {
        {1.31, -2.0, 3.0},
        {2.6, -4.01, 6.01},
        {0.0, -1.5, 5.5},
    });
    try {
      new LinearSystemSolver().getSolver(nearSingular);
    } catch (SingularMatrixSolverException smse) {
      assertEquals(2, smse.getApparentRank());
    }
  }

}
