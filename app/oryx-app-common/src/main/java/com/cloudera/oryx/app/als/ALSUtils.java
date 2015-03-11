/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
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

package com.cloudera.oryx.app.als;

public final class ALSUtils {

  private ALSUtils() {}

  public static double implicitTargetQui(double value, double currentValue) {
    // Target is really 1, or 0, depending on whether value is positive or negative.
    // This wouldn't account for the strength though. Instead the target is a function
    // of the current value and strength. If the current value is c, and value is positive
    // then the target is somewhere between c and 1 depending on the strength. If current
    // value is already >= 1, there's no effect. Similarly for negative values.
    if (value > 0.0f && currentValue < 1.0) {
      double diff = 1.0 - Math.max(0.0, currentValue);
      return currentValue + (1.0 - 1.0 / (1.0 + value)) * diff;
    }
    if (value < 0.0f && currentValue > 0.0) {
      double diff = -Math.min(1.0, currentValue);
      return currentValue + (1.0 - 1.0 / (1.0 - value)) * diff;
    }
    // No change
    return Double.NaN;
  }

}
