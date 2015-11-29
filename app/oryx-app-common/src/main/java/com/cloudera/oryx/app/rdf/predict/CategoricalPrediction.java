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

package com.cloudera.oryx.app.rdf.predict;

import java.util.Arrays;

import com.google.common.base.Preconditions;

import com.cloudera.oryx.app.rdf.example.CategoricalFeature;
import com.cloudera.oryx.app.rdf.example.Example;
import com.cloudera.oryx.app.rdf.example.FeatureType;

/**
 * Represents a prediction of the value of a categorical target. The prediction is not just a single category
 * value, but a probability distribution over all known category values.
 *
 * @see NumericPrediction
 */
public final class CategoricalPrediction extends Prediction {

  /** "counts" can be fractional, or less than 1 */
  private final double[] categoryCounts;
  /** Normalized categoryCounts */
  private volatile double[] categoryProbabilities;
  private volatile int maxCategory;

  public CategoricalPrediction(int[] categoryCounts) {
    this(toDoubles(categoryCounts));
  }

  private static double[] toDoubles(int[] values) {
    double[] result = new double[values.length];
    for (int i = 0; i < result.length; i++) {
      result[i] = values[i];
    }
    return result;
  }

  /**
   * @param categoryCounts "counts" for each category, which may be fractional
   */
  public CategoricalPrediction(double[] categoryCounts) {
    super((int) Math.round(sum(categoryCounts)));
    Preconditions.checkArgument(sum(categoryCounts) > 0.0);
    this.categoryCounts = categoryCounts;
    recompute();
  }

  private static double sum(double[] categoryCounts) {
    double total = 0.0;
    for (double count : categoryCounts) {
      total += count;
    }
    return total;
  }

  private synchronized void recompute() {
    double total = sum(categoryCounts);
    double maxCount = Double.NEGATIVE_INFINITY;
    int theMaxCategory = -1;
    double[] newCategoryProbabilities = new double[categoryCounts.length];
    for (int i = 0; i < newCategoryProbabilities.length; i++) {
      double count = categoryCounts[i];
      if (count > maxCount) {
        maxCount = count;
        theMaxCategory = i;
      }
      newCategoryProbabilities[i] = count / total;
    }
    Preconditions.checkArgument(theMaxCategory >= 0);
    categoryProbabilities = newCategoryProbabilities;
    maxCategory = theMaxCategory;
  }

  public double[] getCategoryCounts() {
    return categoryCounts;
  }

  public double[] getCategoryProbabilities() {
    return categoryProbabilities;
  }

  public int getMostProbableCategoryEncoding() {
    return maxCategory;
  }

  /**
   * @return {@link FeatureType#CATEGORICAL}
   */
  @Override
  public FeatureType getFeatureType() {
    return FeatureType.CATEGORICAL;
  }

  @Override
  public void update(Example train) {
    CategoricalFeature target = (CategoricalFeature) train.getTarget();
    update(target.getEncoding(), 1);
  }

  public synchronized void update(int encoding, int count) {
    categoryCounts[encoding] += count;
    setCount(getCount() + count);
    recompute();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof CategoricalPrediction)) {
      return false;
    }
    CategoricalPrediction other = (CategoricalPrediction) o;
    return Arrays.equals(categoryCounts, other.categoryCounts);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(categoryCounts);
  }

  @Override
  public String toString() {
    return ':' + Arrays.toString(categoryProbabilities);
  }

}
