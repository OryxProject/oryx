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

/**
 * Throws when a system can't be solved because the input matrix is singular or
 * near-singular. Encapsulates its apparent rank too.
 */
public final class SingularMatrixSolverException extends RuntimeException {
  
  private final int apparentRank;
  
  public SingularMatrixSolverException(int apparentRank, String message) {
    super(message);
    this.apparentRank = apparentRank;
  }
  
  public int getApparentRank() {
    return apparentRank;
  }
  
}
