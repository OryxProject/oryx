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

import com.google.common.base.Preconditions;
import org.apache.commons.math3.util.FastMath;

/**
 * Utility class with simple vector-related operations.
 */
public final class VectorMath {

  private VectorMath() {
  }

  /**
   * @return dot product of the two given arrays
   * @param x one array
   * @param y the other array
   * @throws IllegalArgumentException if x and y are empty or of different length
   */
  public static double dot(float[] x, float[] y) {
    int length = x.length;
    Preconditions.checkArgument(length > 0 && length == y.length);
    double dot = 0.0;
    for (int i = 0; i < length; i++) {
      dot += (double) x[i] * (double) y[i];
    }
    return dot;
  }

  /**
   * @param x vector for whom norm to be calculated
   * @return the L2 norm of vector x
   * @throws IllegalArgumentException if x is of 0 length
   */
  public static double norm(float[] x) {
    Preconditions.checkArgument(x.length > 0);
    double total = 0.0;
    for (float f : x) {
      total += (double) f * (double) f;
    }
    return FastMath.sqrt(total);
  }
}