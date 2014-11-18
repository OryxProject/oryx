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

package com.cloudera.oryx.ml.speed.als;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.carrotsearch.hppc.ObjectObjectMap;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.carrotsearch.hppc.predicates.ObjectPredicate;
import com.google.common.base.Preconditions;
import org.apache.commons.math3.linear.RealMatrix;

import com.cloudera.oryx.common.collection.AndPredicate;
import com.cloudera.oryx.common.collection.NotContainsPredicate;
import com.cloudera.oryx.common.math.LinearSystemSolver;
import com.cloudera.oryx.common.math.Solver;
import com.cloudera.oryx.common.math.VectorMath;

/**
 * Contains all data structures needed to create near-real-time updates for an
 * ALS-based recommender.
 */
public final class ALSSpeedModel {

  /** User-feature matrix, where row is keyed by user ID string and row is a dense float array. */
  private final ObjectObjectMap<String,float[]> X;
  /** Item-feature matrix, where row is keyed by item ID string and row is a dense float array. */
  private final ObjectObjectMap<String,float[]> Y;
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
    X = new ObjectObjectOpenHashMap<>();
    Y = new ObjectObjectOpenHashMap<>();
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
    Lock lock = xLock.readLock();
    lock.lock();
    try {
      return X.get(user);
    } finally {
      lock.unlock();
    }
  }

  public float[] getItemVector(String item) {
    Lock lock = yLock.readLock();
    lock.lock();
    try {
      return Y.get(item);
    } finally {
      lock.unlock();
    }
  }

  public void setUserVector(String user, float[] vector) {
    Preconditions.checkNotNull(vector);
    Preconditions.checkArgument(vector.length == features);
    Lock lock = xLock.writeLock();
    lock.lock();
    try {
      if (X.put(user, vector) == null) {
        // User was actually new
        recentNewUsers.add(user);
      }
    } finally {
      lock.unlock();
    }
  }

  public void setItemVector(String item, float[] vector) {
    Preconditions.checkNotNull(vector);
    Preconditions.checkArgument(vector.length == features);
    Lock lock = yLock.writeLock();
    lock.lock();
    try {
      if (Y.put(item, vector) == null) {
        // Item was actually new
        recentNewItems.add(item);
      }
    } finally {
      lock.unlock();
    }
  }

  public void pruneX(Collection<String> users) {
    // Keep all users in the new model, or, that have been added since last model
    ObjectPredicate<String> predicate = new AndPredicate<>(
        new NotContainsPredicate<>(users), new NotContainsPredicate<>(recentNewUsers));
    Lock lock = xLock.writeLock();
    lock.lock();
    try {
      X.removeAll(predicate);
      recentNewUsers.clear();
    } finally {
      lock.unlock();
    }
  }

  public void pruneY(Collection<String> items) {
    // Keep all items in the new model, or, that have been added since last model
    ObjectPredicate<String> predicate = new AndPredicate<>(
        new NotContainsPredicate<>(items), new NotContainsPredicate<>(recentNewItems));
    Lock lock = yLock.writeLock();
    lock.lock();
    try {
      Y.removeAll(predicate);
      recentNewItems.clear();
    } finally {
      lock.unlock();
    }
  }

  public Solver getXTXSolver() {
    RealMatrix XTX;
    Lock lock = xLock.readLock();
    lock.lock();
    try {
      XTX = VectorMath.transposeTimesSelf(X.values());
    } finally {
      lock.unlock();
    }
    return new LinearSystemSolver().getSolver(XTX);
  }

  public Solver getYTYSolver() {
    RealMatrix YTY;
    Lock lock = yLock.readLock();
    lock.lock();
    try {
      YTY = VectorMath.transposeTimesSelf(Y.values());
    } finally {
      lock.unlock();
    }
    return new LinearSystemSolver().getSolver(YTY);
  }

  @Override
  public String toString() {
    return "ALSSpeedModel[features:" + features +
        ", X:(" + X.size() + " users), Y:(" + Y.size() + " items)]";
  }

}
