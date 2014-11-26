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

import java.util.Collection;

import com.google.common.base.Preconditions;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;

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
   * @return dot product of the two given arrays
   * @param x one array
   * @param y the other array
   * @throws IllegalArgumentException if x and y are empty or of different length
   */
  public static double dot(double[] x, float[] y) {
    int length = x.length;
    Preconditions.checkArgument(length > 0 && length == y.length);
    double dot = 0.0;
    for (int i = 0; i < length; i++) {
      dot += x[i] * (double) y[i];
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
    return Math.sqrt(total);
  }

  /**
   * @param M tall, skinny matrix
   * @return MT * M as a dense matrix
   */
  public static RealMatrix transposeTimesSelf(Collection<float[]> M) {
    if (M == null || M.isEmpty()) {
      return null;
    }
    int features = 0;
    RealMatrix result = null;
    for (float[] vector : M) {
      if (result == null) {
        features = vector.length;
        result = new Array2DRowRealMatrix(features, features);
      }
      for (int row = 0; row < features; row++) {
        float rowValue = vector[row];
        for (int col = 0; col < features; col++) {
          result.addToEntry(row, col, rowValue * vector[col]);
        }
      }
    }
    return result;
  }

  /**
   * @param doubles array of doubles
   * @return array "cast" to floats
   */
  public static float[] toFloats(double... doubles) {
    float[] floats = new float[doubles.length];
    for (int i = 0; i < floats.length; i++) {
      floats[i] = (float) doubles[i];
    }
    return floats;
  }

  /**
   * @param floats array of floats
   * @return array "cast" to doubles
   */
  public static double[] toDoubles(float... floats) {
    double[] doubles = new double[floats.length];
    for (int i = 0; i < doubles.length; i++) {
      doubles[i] = floats[i];
    }
    return doubles;
  }

}