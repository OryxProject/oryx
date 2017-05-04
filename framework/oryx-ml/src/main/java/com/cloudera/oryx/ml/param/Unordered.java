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

package com.cloudera.oryx.ml.param;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import com.google.common.base.Preconditions;
import org.apache.commons.math3.random.RandomDataGenerator;

final class Unordered<T> implements HyperParamValues<T>, Serializable {

  private final List<T> values;

  Unordered(Collection<T> values) {
    Objects.requireNonNull(values);
    Preconditions.checkArgument(!values.isEmpty());
    this.values = new ArrayList<>(values);
  }

  @Override
  public List<T> getTrialValues(int num) {
    Preconditions.checkArgument(num > 0);
    return num < values.size() ? values.subList(0, num) : values;
  }

  /**
   * @param rdg random number generator to use
   * @return a value chosen at random from given values
   */
  @Override
  public T getRandomValue(RandomDataGenerator rdg) {
    return values.get(rdg.nextInt(0, values.size() - 1));
  }

  /**
   * @return number of given values
   */
  @Override
  public long getNumDistinctValues() {
    return values.size();
  }

  @Override
  public String toString() {
    return "Unordered[..." + getTrialValues(3) + "...]";
  }

}
