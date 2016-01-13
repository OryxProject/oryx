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

package com.cloudera.oryx.app.rdf.tree;

import java.io.Serializable;

import com.cloudera.oryx.app.classreg.predict.Prediction;
import com.cloudera.oryx.app.classreg.example.Example;

/**
 * Base interface for decision-tree-based classifiers in this package, whether they be a single
 * {@link DecisionTree} or a collection of them as a {@link DecisionForest}.
 */
public interface TreeBasedClassifier extends Serializable {

  /**
   * @param test example whose target value is to be predicated
   * @return a {@link Prediction} of the target value
   */
  Prediction predict(Example test);

  /**
   * Requests that the implementation update its internal state to reflect a new {@link Example}.
   * In this case, the {@link Example} should carry a target value to learn from.
   *
   * @param train new training example
   */
  void update(Example train);

}
