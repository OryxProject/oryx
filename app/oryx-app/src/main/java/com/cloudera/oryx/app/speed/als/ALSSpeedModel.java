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
import java.util.HashSet;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Preconditions;
import net.openhft.koloboke.collect.map.ObjObjMap;
import net.openhft.koloboke.collect.map.hash.HashObjObjMaps;
import org.apache.commons.math3.linear.RealMatrix;

import com.cloudera.oryx.common.collection.KeyOnlyBiPredicate;
import com.cloudera.oryx.common.collection.NotContainsPredicate;
import com.cloudera.oryx.common.collection.Predicates;
import com.cloudera.oryx.common.lang.AutoLock;
import com.cloudera.oryx.common.math.LinearSystemSolver;
import com.cloudera.oryx.common.math.Solver;
import com.cloudera.oryx.common.math.VectorMath;

/**
 * Contains all data structures needed to create near-real-time updates for an
 * ALS-based recommender.
 */
public final class ALSSpeedModel {

  /** User-feature matrix, where row is keyed by user ID string and row is a dense float array. */
  private final ObjObjMap<String,float[]> X;
  /** Item-feature matrix, where row is keyed by item ID string and row is a dense float array. */
  private final ObjObjMap<String,float[]> Y;
  /** Remembers user IDs added since last model. */
  private final Collection<String> recentNewUsers;
  /** Remembers item IDs added since last model. Partitioned like Y. */
  private final Collection<String> recentNewItems;
  /** Controls access to X, and recentNewUsers. */
  private final ReadWriteLock xLock;
  /** Controls access to Y, and recentNewItems. */
  private final ReadWriteLock yLock;
  /** Whether model uses implicit feedback. */
  private final int features;

  /**
   * Creates an empty model.
   *
   * @param features number of features expected for user/item feature vectors
   */
  ALSSpeedModel(int features) {
    Preconditions.checkArgument(features > 0);
    X = HashObjObjMaps.newMutableMap();
    Y = HashObjObjMaps.newMutableMap();
    recentNewUsers = new HashSet<>();
    recentNewItems = new HashSet<>();
    xLock = new ReentrantReadWriteLock();
    yLock = new ReentrantReadWriteLock();
    this.features = features;
  }

  public int getFeatures() {
    return features;
  }

  public float[] getUserVector(String user) {
    try (AutoLock al = new AutoLock(xLock.readLock())) {
      return X.get(user);
    }
  }

  public float[] getItemVector(String item) {
    try (AutoLock al = new AutoLock(yLock.readLock())) {
      return Y.get(item);
    }
  }

  public void setUserVector(String user, float[] vector) {
    Objects.requireNonNull(vector);
    Preconditions.checkArgument(vector.length == features);
    try (AutoLock al = new AutoLock(xLock.writeLock())) {
      if (X.put(user, vector) == null) {
        // User was actually new
        recentNewUsers.add(user);
      }
    }
  }

  public void setItemVector(String item, float[] vector) {
    Objects.requireNonNull(vector);
    Preconditions.checkArgument(vector.length == features);
    try (AutoLock al = new AutoLock(yLock.writeLock())) {
      if (Y.put(item, vector) == null) {
        // Item was actually new
        recentNewItems.add(item);
      }
    }
  }

  public void pruneX(Collection<String> users) {
    // Keep all users in the new model, or, that have been added since last model
    try (AutoLock al = new AutoLock(xLock.writeLock())) {
      X.removeIf(new KeyOnlyBiPredicate<>(Predicates.and(
          new NotContainsPredicate<>(users), new NotContainsPredicate<>(recentNewUsers))));
      recentNewUsers.clear();
    }
  }

  public void pruneY(Collection<String> items) {
    // Keep all items in the new model, or, that have been added since last model
    try (AutoLock al = new AutoLock(yLock.writeLock())) {
      Y.removeIf(new KeyOnlyBiPredicate<>(Predicates.and(
          new NotContainsPredicate<>(items), new NotContainsPredicate<>(recentNewItems))));
      recentNewItems.clear();
    }
  }

  public Solver getXTXSolver() {
    RealMatrix XTX;
    try (AutoLock al = new AutoLock(xLock.readLock())) {
      XTX = VectorMath.transposeTimesSelf(X.values());
    }
    return LinearSystemSolver.getSolver(XTX);
  }

  public Solver getYTYSolver() {
    RealMatrix YTY;
    try (AutoLock al = new AutoLock(yLock.readLock())) {
      YTY = VectorMath.transposeTimesSelf(Y.values());
    }
    return LinearSystemSolver.getSolver(YTY);
  }

  @Override
  public String toString() {
    return "ALSSpeedModel[features:" + features +
        ", X:(" + X.size() + " users), Y:(" + Y.size() + " items)]";
  }

}
