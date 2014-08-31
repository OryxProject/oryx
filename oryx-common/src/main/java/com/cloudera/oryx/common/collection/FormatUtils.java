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

package com.cloudera.oryx.common.collection;

import java.util.regex.Pattern;

import com.google.common.base.Preconditions;

/**
 * Simple formatting-related utility methods.
 */
public final class FormatUtils {

  private static final Pattern COMMA_SPLIT = Pattern.compile(",");

  private FormatUtils() {}

  public static String formatFloatVec(float... vector) {
    StringBuilder formatted = new StringBuilder(11 * vector.length);
    for (int i = 0; i < vector.length; i++) {
      if (i > 0) {
        formatted.append(',');
      }
      formatted.append(vector[i]);
    }
    return formatted.toString();
  }

  public static String formatDoubleVec(double... vector) {
    StringBuilder formatted = new StringBuilder(11 * vector.length);
    for (int i = 0; i < vector.length; i++) {
      if (i > 0) {
        formatted.append(',');
      }
      // Only need float
      formatted.append((float) vector[i]);
    }
    return formatted.toString();
  }

  /**
   * @param s comma-separated floats
   * @return array of floats
   * @throws IllegalArgumentException if any value is infinite or NaN
   */
  public static float[] parseFloatVec(String s) {
    String[] vectorTokens = COMMA_SPLIT.split(s);
    float[] vector = new float[vectorTokens.length];
    for (int i = 0; i < vectorTokens.length; i++) {
      float f = Float.parseFloat(vectorTokens[i]);
      Preconditions.checkArgument(!Float.isInfinite(f) && !Float.isNaN(f));
      vector[i] = f;
    }
    return vector;
  }

  /**
   * @param s comma-separated doubles
   * @return array of doubles
   * @throws IllegalArgumentException if any value is infinite or NaN
   */
  public static double[] parseDoubleVec(String s) {
    String[] vectorTokens = COMMA_SPLIT.split(s);
    double[] vector = new double[vectorTokens.length];
    for (int i = 0; i < vectorTokens.length; i++) {
      double d = Double.parseDouble(vectorTokens[i]);
      Preconditions.checkArgument(!Double.isInfinite(d) && !Double.isNaN(d));
      vector[i] = d;
    }
    return vector;
  }

}
