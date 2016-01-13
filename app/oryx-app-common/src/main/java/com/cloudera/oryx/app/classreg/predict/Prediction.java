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

package com.cloudera.oryx.app.classreg.predict;

import java.io.Serializable;

import com.cloudera.oryx.app.classreg.example.Example;
import com.cloudera.oryx.app.classreg.example.FeatureType;

/**
 * Subclasses represent the predicted value of the target.
 *
 * @see NumericPrediction
 * @see CategoricalPrediction
 */
public abstract class Prediction implements Serializable {

  private volatile int count;

  Prediction(int initialCount) {
    this.count = initialCount;
  }

  public final int getCount() {
    return count;
  }

  final void setCount(int count) {
    this.count = count;
  }

  public abstract FeatureType getFeatureType();

  public abstract void update(Example train);

  @Override
  public abstract String toString();

}
