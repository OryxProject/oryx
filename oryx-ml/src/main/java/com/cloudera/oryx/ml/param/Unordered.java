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
import java.util.List;

import com.google.common.base.Preconditions;

final class Unordered<T> implements HyperParamValues<T>, Serializable {

  private final List<T> values;

  Unordered(List<T> values) {
    Preconditions.checkNotNull(values);
    Preconditions.checkArgument(!values.isEmpty());
    this.values = values;
  }

  @Override
  public List<T> getTrialValues(int num) {
    Preconditions.checkArgument(num > 0);
    return num < values.size() ? values.subList(0, num) : values;
  }

  @Override
  public String toString() {
    return "Unordered[..." + getTrialValues(3) + "...]";
  }

}
