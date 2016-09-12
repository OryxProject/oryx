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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.koloboke.collect.set.ObjSet;
import com.koloboke.collect.set.hash.HashObjSets;

import com.cloudera.oryx.api.speed.SpeedModel;
import com.cloudera.oryx.app.als.FeatureVectors;
import com.cloudera.oryx.app.als.PartitionedFeatureVectors;
import com.cloudera.oryx.app.als.SolverCache;
import com.cloudera.oryx.common.lang.AutoLock;
import com.cloudera.oryx.common.lang.AutoReadWriteLock;
import com.cloudera.oryx.common.math.Solver;

/**
 * Contains all data structures needed to create near-real-time updates for an
 * ALS-based recommender.
 */
public final class ALSSpeedModel implements SpeedModel {

  private static final ExecutorService executor = Executors.newCachedThreadPool(
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ALSSpeedModel-%d").build());

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
  private final boolean logStrength;
  private final double epsilon;
  private final SolverCache cachedXTXSolver;
  private final SolverCache cachedYTYSolver;

  /**
   * Creates an empty model.
   *
   * @param features number of features expected for user/item feature vectors
   * @param implicit whether model implements implicit feedback
   * @param logStrength whether input strengths are log transformed
   * @param epsilon eps in log transform log(1 + r/eps)
   */
  ALSSpeedModel(int features, boolean implicit, boolean logStrength, double epsilon) {
    Preconditions.checkArgument(features > 0);
    int numPartitions = Runtime.getRuntime().availableProcessors();
    X = new PartitionedFeatureVectors(numPartitions, executor);
    Y = new PartitionedFeatureVectors(numPartitions, executor);
    expectedUserIDs = HashObjSets.newMutableSet();
    expectedUserIDsLock = new AutoReadWriteLock();
    expectedItemIDs = HashObjSets.newMutableSet();
    expectedItemIDsLock = new AutoReadWriteLock();
    this.features = features;
    this.implicit = implicit;
    this.logStrength = logStrength;
    this.epsilon = epsilon;
    cachedXTXSolver = new SolverCache(executor, X);
    cachedYTYSolver = new SolverCache(executor, Y);
  }

  public int getFeatures() {
    return features;
  }

  public boolean isImplicit() {
    return implicit;
  }

  public boolean isLogStrength() {
    return logStrength;
  }

  public double getEpsilon() {
    return epsilon;
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
    cachedXTXSolver.setDirty();
  }

  public void setItemVector(String item, float[] vector) {
    Preconditions.checkArgument(vector.length == features);
    Y.setVector(item, vector);
    try (AutoLock al = expectedItemIDsLock.autoWriteLock()) {
      expectedItemIDs.remove(item);
    }
    cachedYTYSolver.setDirty();
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

  void precomputeSolvers() {
    cachedXTXSolver.compute();
    cachedYTYSolver.compute();
  }

  public Solver getXTXSolver() {
    return cachedXTXSolver.get(false);
  }

  public Solver getYTYSolver() {
    return cachedYTYSolver.get(false);
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
        ", logStrength:" + logStrength + ", epsilon:" + epsilon +
        ", X:(" + X.size() + " users), Y:(" + Y.size() + " items), fractionLoaded:" +
        getFractionLoaded() + "]";
  }

}
