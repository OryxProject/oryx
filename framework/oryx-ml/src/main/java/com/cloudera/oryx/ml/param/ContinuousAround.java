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
import java.util.Collections;
import java.util.List;

import com.google.common.base.Preconditions;

final class ContinuousAround implements HyperParamValues<Double>, Serializable {

  private final double around;
  private final double step;

  ContinuousAround(double around, double step) {
    Preconditions.checkArgument(step > 0.0);
    this.around = around;
    this.step = step;
  }

  @Override
  public List<Double> getTrialValues(int num) {
    Preconditions.checkArgument(num > 0);
    if (num == 1) {
      return Collections.singletonList(around);
    }
    List<Double> values = new ArrayList<>(num);
    double value = around - ((num - 1.0) / 2.0) * step;
    for (int i = 0; i < num; i++) {
      values.add(value);
      value += step;
    }
    // Make sure middle value is exact
    if (num % 2 != 0) {
      values.set(num / 2, around);
    }
    return values;
  }

  @Override
  public String toString() {
    return "ContinuousAround[..." + getTrialValues(3) + "...]";
  }

}
