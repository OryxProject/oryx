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

package com.cloudera.oryx.app.classreg.example;

/**
 * Represents the value of a numeric feature -- one that takes on a real value, whether conceptually discrete or
 * continuous. For example both integer values like "number of children" and continuous values like "average age"
 * are represented by this class.
 *
 * @see CategoricalFeature
 */
public final class NumericFeature implements Feature {

  // Optimization for the common case of a feature value of 0
  private static final NumericFeature ZERO = new NumericFeature(0.0);

  private final double value;

  private NumericFeature(double value) {
    this.value = value;
  }

  /**
   * @param value value to represent as a feature
   * @return {@code NumericFeature} representing the given numeric value
   */
  public static NumericFeature forValue(double value) {
    return value == 0.0f ? ZERO : new NumericFeature(value);
  }

  /**
   * @return numeric feature value
   */
  public double getValue() {
    return value;
  }

  /**
   * @return {@link FeatureType#NUMERIC}
   */
  @Override
  public FeatureType getFeatureType() {
    return FeatureType.NUMERIC;
  }
  
  @Override
  public String toString() {
    return Double.toString(value);
  }
  
  @Override
  public int hashCode() {
    return Double.hashCode(value);
  }
  
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof NumericFeature)) {
      return false;
    }
    NumericFeature other = (NumericFeature) o;
    return value == other.value;
  }

}
