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

import java.util.BitSet;

import com.cloudera.oryx.app.rdf.example.CategoricalFeature;
import com.cloudera.oryx.app.rdf.example.Example;
import com.cloudera.oryx.app.rdf.example.FeatureType;

/**
 * Represents a decision over a categorical feature. If the categorical feature takes on
 * values from the set V = { v1, v2, ... } then the rule is defined by a subset of V.
 * The decision evaluates as positive if an example's value for this feature is in the
 * rule's subset.
 *
 * @see NumericDecision
 */
public final class CategoricalDecision extends Decision {
  
  private final BitSet activeCategoryEncodings;
  private final boolean defaultDecision;

  public CategoricalDecision(int featureNumber,
                             BitSet activeCategoryEncodings,
                             boolean defaultDecision) {
    super(featureNumber);
    this.activeCategoryEncodings = activeCategoryEncodings;
    this.defaultDecision = defaultDecision;
  }

  @Override
  public boolean getDefaultDecision() {
    return defaultDecision;
  }

  public BitSet getActiveCategoryEncodings() {
    return activeCategoryEncodings;
  }

  @Override
  public boolean isPositive(Example example) {
    CategoricalFeature feature = (CategoricalFeature) example.getFeature(getFeatureNumber());
    if (feature == null) {
      return defaultDecision;
    }
    int encoding = feature.getEncoding();
    if (encoding >= activeCategoryEncodings.size()) {
      return defaultDecision;
    }
  return activeCategoryEncodings.get(encoding);
  }

  @Override
  public FeatureType getType() {
    return FeatureType.CATEGORICAL;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof CategoricalDecision)) {
      return false;
    }
    CategoricalDecision other = (CategoricalDecision) o;
    return getFeatureNumber() == other.getFeatureNumber() &&
        activeCategoryEncodings.equals(other.activeCategoryEncodings);
  }

  @Override
  public int hashCode() {
    return getFeatureNumber() ^ activeCategoryEncodings.hashCode();
  }
  
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append("(#").append(getFeatureNumber()).append(" ∈ [");
    int category = -1;
    boolean first = true;
    while ((category = activeCategoryEncodings.nextSetBit(category + 1)) >= 0) {
      if (first) {
        first = false;
      } else {
        result.append(',');
      }
      result.append(category);
    }
    result.append("])");
    return result.toString();
  }

}