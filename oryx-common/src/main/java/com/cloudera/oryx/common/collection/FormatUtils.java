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

import com.google.common.base.Joiner;

/**
 * Simple formatting-related utility methods.
 */
public final class FormatUtils {

  private static final Pattern COMMA_SPLIT = Pattern.compile(",");
  private static final Joiner COMMA_JOINER = Joiner.on(',');

  private FormatUtils() {}

  public static String formatFloatVec(float... vector) {
    // Joiner needs a Object[], so go ahead and make strings:
    String[] objVector = new String[vector.length];
    for (int i = 0; i < vector.length; i++) {
      // Only need floats
      objVector[i] = Float.toString(vector[i]);
    }
    return COMMA_JOINER.join(objVector);
  }

  public static String formatDoubleVec(double... vector) {
    // Joiner needs a Object[], so go ahead and make strings:
    String[] objVector = new String[vector.length];
    for (int i = 0; i < vector.length; i++) {
      // Only need floats
      objVector[i] = Float.toString((float) vector[i]);
    }
    return COMMA_JOINER.join(objVector);
  }

  public static float[] parseFloatVec(String s) {
    String[] vectorTokens = COMMA_SPLIT.split(s);
    float[] vector = new float[vectorTokens.length];
    for (int i = 0; i < vectorTokens.length; i++) {
      vector[i] = Float.parseFloat(vectorTokens[i]);
    }
    return vector;
  }

  public static double[] parseDoubleVec(String s) {
    String[] vectorTokens = COMMA_SPLIT.split(s);
    double[] vector = new double[vectorTokens.length];
    for (int i = 0; i < vectorTokens.length; i++) {
      vector[i] = Double.parseDouble(vectorTokens[i]);
    }
    return vector;
  }

}
