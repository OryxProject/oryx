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

import com.github.fommil.netlib.BLAS;
import org.apache.commons.math3.random.RandomGenerator;

/**
 * Utility class with simple vector-related operations.
 */
public final class VectorMath {

  private static final BLAS blas = BLAS.getInstance();

  private VectorMath() {}

  /**
   * @return dot product of the two given arrays
   * @param x one array
   * @param y the other array
   */
  public static double dot(float[] x, float[] y) {
    int length = x.length;
    double dot = 0.0;
    for (int i = 0; i < length; i++) {
      dot += (double) x[i] * y[i];
    }
    return dot;
  }

  /**
   * @param x vector for whom norm to be calculated
   * @return the L2 norm of vector x
   */
  public static double norm(float[] x) {
    double total = 0.0;
    for (float f : x) {
      total += (double) f * f;
    }
    return Math.sqrt(total);
  }

  /**
   * @param x vector for whom norm to be calculated
   * @return the L2 norm of vector x
   */
  public static double norm(double[] x) {
    double total = 0.0;
    for (double d : x) {
      total += d * d;
    }
    return Math.sqrt(total);
  }

  /**
   * Computes cosine similarity of values in two given arrays, when the norm of one array is
   * known in advance, which is a not-uncommon case.
   *
   * @param x one array
   * @param y the other array
   * @param normY norm of y
   * @return cosine similarity = dot(x,y) / (norm(x) * norm(y))
   */
  public static double cosineSimilarity(float[] x, float[] y, double normY) {
    int length = x.length;
    double dot = 0.0;
    double totalXSq = 0.0;
    for (int i = 0; i < length; i++) {
      double xi = x[i];
      totalXSq += xi * xi;
      dot += xi * y[i];
    }
    return dot / (Math.sqrt(totalXSq) * normY);
  }

  /**
   * @param M tall, skinny matrix
   * @return MT * M as a dense lower-triangular matrix, represented in packed row-major form.
   */
  public static double[] transposeTimesSelf(Collection<float[]> M) {
    if (M == null || M.isEmpty()) {
      return null;
    }
    int features = M.iterator().next().length;
    double[] result = new double[features * (features + 1) / 2];
    double[] vectorD = new double[features];
    for (float[] vector : M) {
      // Unfortunately need to copy into a double[]
      for (int i = 0; i < vector.length; i++) {
        vectorD[i] = vector[i];
      }
      blas.dspr("L", features, 1.0, vectorD, 1, result);
    }
    return result;
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

}