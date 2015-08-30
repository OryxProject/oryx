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

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.random.RandomGenerator;

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
  public static double dot(double[] x, double[] y) {
    int length = x.length;
    double dot = 0.0;
    for (int i = 0; i < length; i++) {
      dot += x[i] * y[i];
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
    double total = 0.0;
    for (float f : x) {
      double d = (double) f;
      total += d * d;
    }
    return Math.sqrt(total);
  }

  /**
   * @param x vector for whom norm to be calculated
   * @return the L2 norm of vector x
   * @throws IllegalArgumentException if x is of 0 length
   */
  public static double norm(double[] x) {
    double total = 0.0;
    for (double d : x) {
      total += d * d;
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

  /**
   * @param values numeric values as {@link String}s
   * @return values parsed as {@code double[]}
   */
  public static double[] parseVector(String[] values) {
    double[] doubles = new double[values.length];
    for (int i = 0; i < values.length; i++) {
      doubles[i] = Double.parseDouble(values[i]);
    }
    return doubles;
  }

  /**
   * @param features dimension of vector
   * @param random random number generator
   * @return vector whose direction from the origin is chosen uniformly at random, but which is not normalized
   */
  public static float[] randomVectorF(int features, RandomGenerator random) {
    float[] vector = new float[features];
    for (int i = 0; i < features; i++) {
      vector[i] = (float) random.nextGaussian();
    }
    return vector;
  }

  /**
   * @param features dimension of vector
   * @param random random number generator
   * @return vector whose direction from the origin is chosen uniformly at random, but which is not normalized
   */
  public static double[] randomVectorD(int features, RandomGenerator random) {
    double[] vector = new double[features];
    for (int i = 0; i < features; i++) {
      vector[i] = random.nextGaussian();
    }
    return vector;
  }

}