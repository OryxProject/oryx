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

import com.cloudera.oryx.app.rdf.predict.Prediction;
import com.cloudera.oryx.app.rdf.example.Example;

/**
 * Represents a leaf node in a {@link DecisionTree}, which contains a {@link Prediction} for a target
 * rather than a decision over an example.
 *
 * @see DecisionNode
 */
public final class TerminalNode extends TreeNode {

  private final Prediction prediction;

  public TerminalNode(String id, Prediction prediction) {
    super(id);
    this.prediction = prediction;
  }

  public Prediction getPrediction() {
    return prediction;
  }

  public int getCount() {
    return prediction.getCount();
  }

  public void update(Example train) {
    prediction.update(train);
  }

  @Override
  public boolean isTerminal() {
    return true;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof TerminalNode)) {
      return false;
    }
    TerminalNode other = (TerminalNode) o;
    return prediction.equals(other.prediction);
  }

  @Override
  public int hashCode() {
    return prediction.hashCode();
  }

  @Override
  public String toString() {
    return "[ " + prediction + " ]";
  }

}
