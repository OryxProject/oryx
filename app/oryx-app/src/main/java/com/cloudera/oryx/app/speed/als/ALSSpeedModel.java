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
import net.openhft.koloboke.collect.set.ObjSet;
import net.openhft.koloboke.collect.set.hash.HashObjSets;

import com.cloudera.oryx.api.speed.SpeedModel;
import com.cloudera.oryx.app.als.FeatureVectors;
import com.cloudera.oryx.common.lang.AutoLock;
import com.cloudera.oryx.common.lang.AutoReadWriteLock;
import com.cloudera.oryx.common.math.LinearSystemSolver;
import com.cloudera.oryx.common.math.Solver;

/**
 * Contains all data structures needed to create near-real-time updates for an
 * ALS-based recommender.
 */
public final class ALSSpeedModel implements SpeedModel {

  /** User-feature matrix. */
  private final FeatureVectors X;
  /** Item-feature matrix. */
  private final FeatureVectors Y;
  private final ObjSet<String> expectedUserIDs;
  private final AutoReadWriteLock expectedUserIDsLock;
  private final ObjSet<String> expectedItemIDs;
  private final AutoReadWriteLock expectedItemIDsLock;
  /** Number of features used in the model. */
  private final int features;
  /** Whether model uses implicit feedback. */
  private final boolean implicit;

  /**
   * Creates an empty model.
   *
   * @param features number of features expected for user/item feature vectors
   * @param implicit whether model implements implicit feedback
   */
  ALSSpeedModel(int features, boolean implicit) {
    Preconditions.checkArgument(features > 0);
    X = new FeatureVectors();
    Y = new FeatureVectors();
    expectedUserIDs = HashObjSets.newMutableSet();
    expectedUserIDsLock = new AutoReadWriteLock();
    expectedItemIDs = HashObjSets.newMutableSet();
    expectedItemIDsLock = new AutoReadWriteLock();
    this.features = features;
    this.implicit = implicit;
  }

  public int getFeatures() {
    return features;
  }

  public boolean isImplicit() {
    return implicit;
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
    try (AutoLock al = expectedUserIDsLock.autoWriteLock()) {
      expectedUserIDs.remove(user);
    }
  }

  public void setItemVector(String item, float[] vector) {
    Preconditions.checkArgument(vector.length == features);
    Y.setVector(item, vector);
    try (AutoLock al = expectedItemIDsLock.autoWriteLock()) {
      expectedItemIDs.remove(item);
    }
  }

  public void retainRecentAndUserIDs(Collection<String> users) {
    X.retainRecentAndIDs(users);
    try (AutoLock al = expectedUserIDsLock.autoWriteLock()) {
      expectedUserIDs.clear();
      expectedUserIDs.addAll(users);
      X.removeAllIDsFrom(expectedUserIDs);
    }
  }

  public void retainRecentAndItemIDs(Collection<String> items) {
    Y.retainRecentAndIDs(items);
    try (AutoLock al = expectedItemIDsLock.autoWriteLock()) {
      expectedItemIDs.clear();
      expectedItemIDs.addAll(items);
      Y.removeAllIDsFrom(expectedItemIDs);
    }
  }

  public Solver getXTXSolver() {
    // Not cached now, since the way it is used now, it is accessed once per batch of input anyway
    return LinearSystemSolver.getSolver(X.getVTV());
  }

  public Solver getYTYSolver() {
    // Not cached now, since the way it is used now, it is accessed once per batch of input anyway
    return LinearSystemSolver.getSolver(Y.getVTV());
  }

  @Override
  public float getFractionLoaded() {
    int expected = 0;
    try (AutoLock al = expectedUserIDsLock.autoReadLock()) {
      expected += expectedUserIDs.size();
    }
    try (AutoLock al = expectedItemIDsLock.autoReadLock()) {
      expected += expectedItemIDs.size();
    }
    if (expected == 0) {
      return 1.0f;
    }
    float loaded = (float) X.size() + Y.size();
    return loaded / (loaded + expected);
  }

  @Override
  public String toString() {
    return "ALSSpeedModel[features:" + features + ", implicit:" + implicit +
        ", X:(" + X.size() + " users), Y:(" + Y.size() + " items), fractionLoaded:" +
        getFractionLoaded() + "]";
  }

}
