/*
 * Copyright (c) 2014, Cloudera, Inc. and Intel Corp. All Rights Reserved.
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

package com.cloudera.oryx.ml.serving.als.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.carrotsearch.hppc.ObjectObjectMap;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.base.Preconditions;
import org.apache.commons.math3.linear.RealMatrix;

import com.cloudera.oryx.common.collection.NotContainsPredicate;
import com.cloudera.oryx.common.math.LinearSystemSolver;
import com.cloudera.oryx.common.math.Solver;
import com.cloudera.oryx.common.math.VectorMath;
import com.cloudera.oryx.lambda.serving.ServingModel;

public final class ALSServingModel implements ServingModel {

  private final ObjectObjectMap<String,float[]> X;
  private final ObjectObjectMap<String,float[]> Y;
  private final ReadWriteLock xLock;
  private final ReadWriteLock yLock;
  private final int features;

  ALSServingModel(int features) {
    Preconditions.checkArgument(features > 0);
    X = new ObjectObjectOpenHashMap<>(10000);
    Y = new ObjectObjectOpenHashMap<>(10000);
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

  void setUserVector(String user, float[] vector) {
    Preconditions.checkNotNull(vector);
    Preconditions.checkArgument(vector.length == features);
    Lock lock = xLock.writeLock();
    lock.lock();
    try {
      X.put(user, vector);
    } finally {
      lock.unlock();
    }
  }

  public Collection<String> getAllItemIDs() {
    Lock lock = yLock.readLock();
    lock.lock();
    try {
      Collection<String> itemsList = new ArrayList<>(Y.size());
      for (ObjectCursor<String> intCursor : Y.keys()) {
        itemsList.add(intCursor.value);
      }
      return itemsList;
    } finally {
      lock.unlock();
    }
  }

  /**
   * @param user user ID
   * @param items one or more item IDs
   * @return dot product between user vector and one or more item vectors
   */
  public double[] dotProduct(String user, String... items) {
    float[] userFeatures = getUserVector(user);
    double[] results = new double[items.length];
    for (int i = 0; i < items.length; i++) {
      float[] itemFeatures = getItemVector(items[i]);
      if (itemFeatures != null) {
        double value = VectorMath.dot(itemFeatures, userFeatures);
        Preconditions.checkState(!(Double.isInfinite(value) || Double.isNaN(value)), "Bad estimate");
        results[i] = value;
      }
    }
    return results;
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

  void setItemVector(String item, float[] vector) {
    Preconditions.checkNotNull(vector);
    Preconditions.checkArgument(vector.length == features);
    Lock lock = yLock.writeLock();
    lock.lock();
    try {
      Y.put(item, vector);
    } finally {
      lock.unlock();
    }
  }

  void retainAllUsers(Collection<String> users) {
    Lock lock = xLock.writeLock();
    lock.lock();
    try {
      X.removeAll(new NotContainsPredicate<>(users));
    } finally {
      lock.unlock();
    }
  }

  void retainAllItems(Collection<String> items) {
    Lock lock = yLock.writeLock();
    lock.lock();
    try {
      Y.removeAll(new NotContainsPredicate<>(items));
    } finally {
      lock.unlock();
    }
  }

  public double dotProduct(String toItemID, String[] itemIDs, float[] values) {
    float[] toItemFeatures = getItemVector(toItemID);
    float[] anonymousUserFeatures = buildAnonymousUserFeatures(itemIDs, values);
    return VectorMath.dot(anonymousUserFeatures, toItemFeatures);
  }

  private float[] buildAnonymousUserFeatures(String[] itemIDs, float[] values) {
    Preconditions.checkArgument(values == null || values.length == itemIDs.length,
        "Number of values doesn't match number of items");

    Solver ytySolver = getYTYSolver();
    float[] anonymousUserFeatures = null;
    for (int j = 0; j < itemIDs.length; j++) {
      String itemID = itemIDs[j];
      float[] itemFeatures = getItemVector(itemID);
      double[] userFoldIn = ytySolver.solveFToD(itemFeatures);
      if (anonymousUserFeatures == null) {
        anonymousUserFeatures = new float[userFoldIn.length];
      }
      double signedFoldInWeight = foldInWeight(0.0, values == null ? 1.0f : values[j]);
      if (signedFoldInWeight != 0.0) {
        for (int i = 0; i < anonymousUserFeatures.length; i++) {
          anonymousUserFeatures[i] += (float) (signedFoldInWeight * userFoldIn[i]);
        }
      }
    }
    return anonymousUserFeatures;
  }

  /**
   * This function decides how much of a folded-in user or item vector should be added to a target item or user
   * vector, respectively, on a new action. The idea is that a positive value should push the current value towards
   * 1, but not further, and a negative value should push towards 0, but not further. How much to move should be
   * mostly proportional to the size of the value. 0 should move the result not at all; 2 ought to move twice as
   * much as 1, etc. This isn't quite possible but can be approximated by moving a fraction 1-1/(1+value) of the
   * distance towards 1, or 0.
   */
  private static double foldInWeight(double estimate, float value) {
    Preconditions.checkState(!(Double.isInfinite(estimate) || Double.isNaN(estimate)));
    double signedFoldInWeight;
    if (value > 0.0f && estimate < 1.0) {
      double multiplier = 1.0 - Math.max(0.0, estimate);
      signedFoldInWeight = (1.0 - 1.0 / (1.0 + value)) * multiplier;
    } else if (value < 0.0f && estimate > 0.0) {
      double multiplier = -Math.min(1.0, estimate);
      signedFoldInWeight = (1.0 - 1.0 / (1.0 - value)) * multiplier;
    } else {
      signedFoldInWeight = 0.0;
    }
    return signedFoldInWeight;
  }
}
