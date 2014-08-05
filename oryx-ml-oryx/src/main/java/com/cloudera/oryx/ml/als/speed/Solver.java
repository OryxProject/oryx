/*
 * Copyright (c) 2013, Cloudera, Inc. All Rights Reserved.
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

package com.cloudera.oryx.ml.als.speed;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.DecompositionSolver;
import org.apache.commons.math3.linear.RealVector;

/**
 * Encapsulates a {@link DecompositionSolver} from Commons Math.
 * 
 * @author Sean Owen
 */
public final class Solver {
  
  private final DecompositionSolver solver;
  
  Solver(DecompositionSolver solver) {
    this.solver = solver;
  }

  public float[] solveFToF(float[] b) {
    RealVector bVec = new ArrayRealVector(b.length);
    for (int i = 0; i < b.length; i++) {
      bVec.setEntry(i, b[i]);
    }
    RealVector vec = solver.solve(bVec);
    float[] result = new float[b.length];
    for (int i = 0; i < result.length; i++) {
      result[i] = (float) vec.getEntry(i);
    }
    return result;
  }

  /*
  public float[] solveDToF(double[] b) {
    RealVector vec = solver.solve(new ArrayRealVector(b, false));
    float[] result = new float[b.length];
    for (int i = 0; i < result.length; i++) {
      result[i] = (float) vec.getEntry(i);
    }
    return result;
  }

  public double[] solveFToD(float[] b) {
    RealVector bVec = new ArrayRealVector(b.length);
    for (int i = 0; i < b.length; i++) {
      bVec.setEntry(i, b[i]);
    }
    RealVector vec = solver.solve(bVec);
    double[] result = new double[b.length];
    for (int i = 0; i < result.length; i++) {
      result[i] = vec.getEntry(i);
    }
    return result;
  }
   */

}
