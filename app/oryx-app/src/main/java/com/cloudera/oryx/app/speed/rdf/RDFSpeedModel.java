/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
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

package com.cloudera.oryx.app.speed.rdf;

import com.google.common.base.Preconditions;

import com.cloudera.oryx.app.rdf.tree.DecisionForest;
import com.cloudera.oryx.app.schema.CategoricalValueEncodings;

/**
 * Contains all data structures needed to create near-real-time updates for a
 * random decision forest-based classifier or regressor.
 */
public final class RDFSpeedModel {

  private final DecisionForest forest;
  private final CategoricalValueEncodings encodings;

  RDFSpeedModel(DecisionForest forest, CategoricalValueEncodings encodings) {
    Preconditions.checkNotNull(forest);
    Preconditions.checkNotNull(encodings);
    this.forest = forest;
    this.encodings = encodings;
  }

  public DecisionForest getForest() {
    return forest;
  }

  public CategoricalValueEncodings getEncodings() {
    return encodings;
  }

  @Override
  public String toString() {
    return "RDFSpeedModel[numTrees:" + forest.getTrees().length + "]";
  }

}
