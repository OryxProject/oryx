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

package com.cloudera.oryx.app.speed.als;

import java.util.Collection;

import com.google.common.base.Preconditions;

import com.cloudera.oryx.app.als.FeatureVectors;
import com.cloudera.oryx.common.math.LinearSystemSolver;
import com.cloudera.oryx.common.math.Solver;

/**
 * Contains all data structures needed to create near-real-time updates for an
 * ALS-based recommender.
 */
public final class ALSSpeedModel {

  /** User-feature matrix. */
  private final FeatureVectors X;
  /** Item-feature matrix. */
  private final FeatureVectors Y;
  /** Whether model uses implicit feedback. */
  private final int features;

  /**
   * Creates an empty model.
   *
   * @param features number of features expected for user/item feature vectors
   */
  ALSSpeedModel(int features) {
    Preconditions.checkArgument(features > 0);
    X = new FeatureVectors();
    Y = new FeatureVectors();
    this.features = features;
  }

  public int getFeatures() {
    return features;
  }

  public float[] getUserVector(String user) {
    return X.getVector(user);
  }

  public float[] getItemVector(String item) {
    return Y.getVector(item);
  }

  public void setUserVector(String user, float[] vector) {
    Preconditions.checkArgument(vector.length == features);
    X.setVector(user, vector);
  }

  public void setItemVector(String item, float[] vector) {
    Preconditions.checkArgument(vector.length == features);
    Y.setVector(item, vector);
  }

  public void pruneX(Collection<String> users) {
    X.prune(users);
  }

  public void pruneY(Collection<String> items) {
    Y.prune(items);
  }

  public Solver getXTXSolver() {
    return LinearSystemSolver.getSolver(X.getVTV());
  }

  public Solver getYTYSolver() {
    return LinearSystemSolver.getSolver(Y.getVTV());
  }

  @Override
  public String toString() {
    return "ALSSpeedModel[features:" + features +
        ", X:(" + X.size() + " users), Y:(" + Y.size() + " items)]";
  }

}
