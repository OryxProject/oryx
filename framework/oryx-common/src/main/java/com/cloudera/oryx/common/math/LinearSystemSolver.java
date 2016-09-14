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

package com.cloudera.oryx.common.math;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.DecompositionSolver;
import org.apache.commons.math3.linear.RRQRDecomposition;
import org.apache.commons.math3.linear.RealMatrix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation based on {@link RRQRDecomposition} from Commons Math.
 */
public final class LinearSystemSolver {
  
  private static final Logger log = LoggerFactory.getLogger(LinearSystemSolver.class);
  private static final double SINGULARITY_THRESHOLD_RATIO = 1.0e-5;

  private LinearSystemSolver() {}

  public static Solver getSolver(double[][] data) {
    if (data == null) {
      return null;
    }
    RealMatrix M = new Array2DRowRealMatrix(data, false);
    double infNorm = M.getNorm();
    double singularityThreshold = infNorm * SINGULARITY_THRESHOLD_RATIO;
    RRQRDecomposition decomposition = new RRQRDecomposition(M, singularityThreshold);
    DecompositionSolver solver = decomposition.getSolver();
    if (solver.isNonSingular()) {
      return new Solver(solver);
    }
    // Otherwise try to report apparent rank
    int apparentRank = decomposition.getRank(0.01); // Better value?
    log.warn("{} x {} matrix is near-singular (threshold {}). Add more data or decrease the " +
             "number of features, to <= about {}",
             M.getRowDimension(), 
             M.getColumnDimension(),
             singularityThreshold,
             apparentRank);
    throw new SingularMatrixSolverException(apparentRank, "Apparent rank: " + apparentRank);
  }

}
