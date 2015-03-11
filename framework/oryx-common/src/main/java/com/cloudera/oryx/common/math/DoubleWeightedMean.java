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

import java.io.Serializable;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;
import org.apache.commons.math3.stat.descriptive.AbstractStorelessUnivariateStatistic;

/**
 * <p>A weighted mean implementation for floating-point weights, following the Commons
 * Math3 framework.</p>
 *
 * <p>This class is not thread-safe.</p>
 */
public final class DoubleWeightedMean
    extends AbstractStorelessUnivariateStatistic implements Serializable {

  private long count;
  private double totalWeight;
  private double mean;

  public DoubleWeightedMean() {
    this(0, 0.0, Double.NaN);
  }

  private DoubleWeightedMean(long count, double totalWeight, double mean) {
    this.count = count;
    this.totalWeight = totalWeight;
    this.mean = mean;
  }

  @Override
  public DoubleWeightedMean copy() {
    return new DoubleWeightedMean(count, totalWeight, mean);
  }

  @Override
  public void clear() {
    count = 0;
    totalWeight = 0.0;
    mean = Double.NaN;
  }

  @Override
  public double getResult() {
    return mean;
  }

  @Override
  public long getN() {
    return count;
  }

  /**
   * @param datum new datum to add to the mean, with weight 1
   */
  @Override
  public void increment(double datum) {
    increment(datum, 1.0);
  }

  /**
   * @param datum new datum to add to the mean
   * @param weight weight of the new datum
   */
  public void increment(double datum, double weight) {
    Preconditions.checkArgument(weight >= 0.0);
    if (count == 0) {
      count = 1;
      mean = datum;
      totalWeight = weight;
    } else {
      count++;
      totalWeight += weight;
      mean += (weight / totalWeight) * (datum - mean);
    }
  }

  @Override
  public String toString() {
    return Double.toString(mean);
  }

  @Override
  public int hashCode() {
    return Longs.hashCode(count) ^ Doubles.hashCode(totalWeight) ^ Doubles.hashCode(mean);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof DoubleWeightedMean)) {
      return false;
    }
    DoubleWeightedMean other = (DoubleWeightedMean) o;
    return count == other.count &&
        Doubles.compare(totalWeight, other.totalWeight) == 0 &&
        Doubles.compare(mean, other.mean) == 0;
  }

}
