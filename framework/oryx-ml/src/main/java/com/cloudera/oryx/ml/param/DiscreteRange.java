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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.commons.math3.random.RandomDataGenerator;

final class DiscreteRange implements HyperParamValues<Integer>, Serializable {

  private final int min;
  private final int max;

  DiscreteRange(int min, int max) {
    Preconditions.checkArgument(min <= max);
    this.min = min;
    this.max = max;
  }

  @Override
  public List<Integer> getTrialValues(int num) {
    Preconditions.checkArgument(num > 0);
    if (max == min) {
      return Collections.singletonList(min);
    }
    if (num == 1) {
      return Collections.singletonList((max + min) / 2);
    }
    if (num == 2) {
      return Arrays.asList(min, max);
    }
    List<Integer> values;
    if (num > (max - min)) {
      values = new ArrayList<>(max - min + 1);
      for (int i = min; i <= max; i++) {
        values.add(i);
      }
    } else {
      values = new ArrayList<>(num);
      double diff = (max - min) / (num - 1.0);
      values.add(min);
      for (int i = 1; i < num - 1; i++) {
        values.add((int) Math.round(values.get(i-1) + diff));
      }
      values.add(max);
    }
    return values;
  }

  /**
   * @param rdg random number generator to use
   * @return a hyperparameter value chosen uniformly at random from the range
   */
  @Override
  public Integer getRandomValue(RandomDataGenerator rdg) {
    if (max == min) {
      return min;
    }
    return rdg.nextInt(min, max);
  }

  /**
   * @return max - min + 1
   */
  @Override
  public long getNumDistinctValues() {
    return (long) max - min + 1;
  }

  @Override
  public String toString() {
    return "DiscreteRange[..." + getTrialValues(3) + "...]";
  }

}
