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
}
