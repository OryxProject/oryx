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

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

public final class LinearSystemSolverTest extends OryxTest {

  @Test
  public void testNull() {
    assertNull(LinearSystemSolver.getSolver((double[]) null));
    assertNull(LinearSystemSolver.getSolver((double[][]) null));
  }

  @Test
  public void testSolveFToF() {
    double[][] a = {
        {1.3, -2.0, 3.0},
        {2.0, 0.0, 5.0},
        {0.0, -1.5, 5.5},
    };
    Solver solver = LinearSystemSolver.getSolver(a);
    assertNotNull(solver);
    float[] y = solver.solveFToF(new float[] {1.0f, 2.0f, 6.5f});
    assertArrayEquals(new float[] {-1.956044f, 0.0021978023f, 1.1824176f}, y);
  }

  @Test
  public void testSolveDToD() {
    double[][] a = {
        {1.3, -2.0, 3.0},
        {2.0, 0.0, 5.0},
        {0.0, -1.5, 5.5},
    };
    Solver solver = LinearSystemSolver.getSolver(a);
    assertNotNull(solver);
    double[] y = solver.solveDToD(new double[] {1.0, 2.0, 6.5});
    assertArrayEquals(
        new double[] {-1.9560439560439564, 0.002197802197802894, 1.1824175824175824}, y);
  }

  @Test
  public void testSolveDToDPacked() {
    double[] a = {1.3, -2.0, 2.0, 3.0, 5.0, 1.5};
    Solver solver = LinearSystemSolver.getSolver(a);
    assertNotNull(solver);
    double[] y = solver.solveDToD(new double[] {1.0, 2.0, 6.5});
    assertArrayEquals(
        new double[] {1.163614884819846, 0.701122268163024, 0.444772593030124}, y);
  }

  @Test
  public void testApparentRank() {
    try {
      LinearSystemSolver.getSolver(new double[][] {
          {1.3001, -2.0, 3.0},
          {2.6, -4.0001, 6.0001},
          {0.0, -1.5, 5.5},
      });
      fail("Expected singular matrix");
    } catch (SingularMatrixSolverException smse) {
      assertEquals(2, smse.getApparentRank());
    }
    try {
      LinearSystemSolver.getSolver(new double[][] {
          {1.3001, -2.0, 3.0},
          {2.6, -4.0001, 6.0001},
          {1.3, -2.0002, 3.0002},
      });
      fail("Expected singular matrix");
    } catch (SingularMatrixSolverException smse) {
      assertEquals(1, smse.getApparentRank());
    }
  }

}
