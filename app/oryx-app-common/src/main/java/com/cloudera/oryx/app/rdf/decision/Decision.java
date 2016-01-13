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

package com.cloudera.oryx.app.rdf.decision;

import java.io.Serializable;

import com.cloudera.oryx.app.classreg.example.Example;
import com.cloudera.oryx.app.classreg.example.FeatureType;

/**
 * Subclasses represent a binary yes/no positive/negative decision based on the value of
 * a feature in an example. This can be a numeric feature or a categorical feature.
 *
 * @see NumericDecision
 * @see CategoricalDecision
 */
public abstract class Decision implements Serializable {

  private final int featureNumber;

  Decision(int featureNumber) {
    this.featureNumber = featureNumber;
  }

  /**
   * @return number of the feature whose value the {@code Decision} operates on
   */
  public final int getFeatureNumber() {
    return featureNumber;
  }

  /**
   * @param example example to evaluate {@code Decision} on
   * @return true iff the {@code Decision} is positive on this example
   */
  public abstract boolean isPositive(Example example);

  /**
   * @return default decision -- true means positive -- when the {@code Decision} cannot be evaluated
   *  on an {@link Example} because the feature value it decides on is missing
   */
  public abstract boolean getDefaultDecision();

  /**
   * @return type of feature the {@code Decision} is over (a {@link FeatureType})
   */
  public abstract FeatureType getType();

  @Override
  public abstract String toString();

}
