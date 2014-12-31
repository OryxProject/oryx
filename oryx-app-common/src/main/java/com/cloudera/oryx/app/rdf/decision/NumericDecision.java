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

import com.cloudera.oryx.app.rdf.example.Example;
import com.cloudera.oryx.app.rdf.example.FeatureType;
import com.cloudera.oryx.app.rdf.example.NumericFeature;
import com.cloudera.oryx.common.lang.LangUtils;

/**
 * Represents a decision over a numeric feature. Decisions are defined simply by a threshold
 * value; the decision is positive if the feature's value is greater than or equal to
 * this decision's threshold value.
 *
 * @see CategoricalDecision
 */
public final class NumericDecision extends Decision {

  private final double threshold;
  private final boolean defaultDecision;

  public NumericDecision(int featureNumber, double threshold, boolean defaultDecision) {
    super(featureNumber);
    this.threshold = threshold;
    this.defaultDecision = defaultDecision;
  }

  private NumericDecision(int featureNumber, double threshold, double mean) {
    super(featureNumber);
    this.threshold = threshold;
    this.defaultDecision = mean >= threshold;
  }

  /**
   * @return decision threshold; feature values greater than or equal are considered positive
   */
  public double getThreshold() {
    return threshold;
  }

  @Override
  public boolean getDefaultDecision() {
    return defaultDecision;
  }

  @Override
  public boolean isPositive(Example example) {
    NumericFeature feature = (NumericFeature) example.getFeature(getFeatureNumber());
    return feature == null ? defaultDecision : feature.getValue() >= threshold;
  }

  @Override
  public FeatureType getType() {
    return FeatureType.NUMERIC;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof NumericDecision)) {
      return false;
    }
    NumericDecision other = (NumericDecision) o;
    return getFeatureNumber() == other.getFeatureNumber() && threshold == other.threshold;
  }

  @Override
  public int hashCode() {
    return getFeatureNumber() ^ LangUtils.hashDouble(threshold);
  }

  @Override
  public String toString() {
    return "(#" + getFeatureNumber() + " >= " + threshold + ')';
  }

}
