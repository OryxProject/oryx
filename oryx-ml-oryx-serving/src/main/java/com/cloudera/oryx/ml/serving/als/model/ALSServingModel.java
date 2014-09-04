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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.carrotsearch.hppc.ObjectObjectMap;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import org.apache.commons.math3.linear.RealMatrix;

import com.cloudera.oryx.common.collection.NotContainsPredicate;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.collection.PairComparators;
import com.cloudera.oryx.common.math.LinearSystemSolver;
import com.cloudera.oryx.common.math.Solver;
import com.cloudera.oryx.common.math.VectorMath;

public final class ALSServingModel {

  private final ObjectObjectMap<String,float[]> X;
  private final ObjectObjectMap<String,float[]> Y;
  private final ObjectObjectMap<String,Collection<String>> knownItems;
  private final ReadWriteLock xLock;
  private final ReadWriteLock yLock;
  private final ReadWriteLock knownItemsLock;
  private final int features;
  private final boolean implicit;

  ALSServingModel(int features, boolean implicit) {
    Preconditions.checkArgument(features > 0);
    X = new ObjectObjectOpenHashMap<>();
    Y = new ObjectObjectOpenHashMap<>();
    knownItems = new ObjectObjectOpenHashMap<>();
    xLock = new ReentrantReadWriteLock();
    yLock = new ReentrantReadWriteLock();
    knownItemsLock = new ReentrantReadWriteLock();
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

  public Collection<String> getKnownItems(String user) {
    Lock lock = this.knownItemsLock.readLock();
    lock.lock();
    try {
      return this.knownItems.get(user);
    } finally {
      lock.unlock();
    }
  }

  void addKnownItems(String user, Collection<String> items) {
    Collection<String> knownItemsForUser;

    Lock lock = this.knownItemsLock.readLock();
    lock.lock();
    try {
      knownItemsForUser = this.knownItems.get(user);
    } finally {
      lock.unlock();
    }

    if (knownItemsForUser == null) {
      Lock writeLock = this.knownItemsLock.writeLock();
      writeLock.lock();
      try {
        // Check again
        knownItemsForUser = this.knownItems.get(user);
        if (knownItemsForUser == null) {
          knownItemsForUser = Collections.synchronizedSet(new HashSet<String>());
          this.knownItems.put(user, knownItemsForUser);
        }
      } finally {
        writeLock.unlock();
      }
    }

    knownItemsForUser.addAll(items);
  }

  public List<Pair<String,Double>> topDotWithUserVector(String user,
                                                        int howMany,
                                                        boolean considerKnownItems) {
    float[] userVector = getUserVector(user);
    if (userVector == null) {
      return null;
    }
    Lock lock = yLock.readLock();
    lock.lock();
    try {
      Iterable<Pair<String,Double>> idDots = Iterables.transform(Y, new DotsFunction(userVector));
      Ordering<Pair<String,Double>> ordering =
          Ordering.from(PairComparators.<String,Double>bySecond());
      return ordering.greatestOf(idDots, howMany);
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

  /**
   * @param users users that should be retained; all else can be removed
   */
  void retainAllUsers(Collection<String> users) {
    Lock lock = xLock.writeLock();
    lock.lock();
    try {
      X.removeAll(new NotContainsPredicate<>(users));
    } finally {
      lock.unlock();
    }
  }

  /**
   * @param items items that should be retained; all else can be removed
   */
  void retainAllItems(Collection<String> items) {
    Lock lock = yLock.writeLock();
    lock.lock();
    try {
      Y.removeAll(new NotContainsPredicate<>(items));
    } finally {
      lock.unlock();
    }
  }

  /**
   * @param items items that should be retained; all else can be removed
   */
  void pruneKnownItems(Collection<String> items) {
    Lock lock = this.knownItemsLock.readLock();
    lock.lock();
    try {
      for (ObjectCursor<Collection<String>> collectionObjectCursor : this.knownItems.values()) {
        // Assuming this collection is thread-safe:
        collectionObjectCursor.value.retainAll(items);
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public String toString() {
    return "ALSServingModel[features:" + features + ", implicit:" + implicit +
        ", X:(" + X.size() + " users), Y:(" + Y.size() + " items), knownItems:(" +
        knownItems.size() + " users)]";
  }

  private static class DotsFunction
      implements Function<ObjectObjectCursor<String,float[]>,Pair<String,Double>> {
    private final float[] userVector;
    DotsFunction(float[] userVector) {
      this.userVector = userVector;
    }
    @Override
    public Pair<String,Double> apply(ObjectObjectCursor<String,float[]> itemIDVector) {
      return new Pair<>(itemIDVector.key, VectorMath.dot(userVector, itemIDVector.value));
    }
  }

}
