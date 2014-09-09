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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.carrotsearch.hppc.ObjectIntMap;
import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import com.carrotsearch.hppc.ObjectObjectMap;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.carrotsearch.hppc.ObjectOpenHashSet;
import com.carrotsearch.hppc.ObjectSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
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
  private final ObjectObjectMap<String,ObjectSet<String>> knownItems;
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

  ObjectSet<String> getKnownItems(String user) {
    Lock lock = this.knownItemsLock.readLock();
    lock.lock();
    try {
      return this.knownItems.get(user);
    } finally {
      lock.unlock();
    }
  }

  public ObjectIntMap<String> getItemCounts() {
    ObjectIntMap<String> counts = new ObjectIntOpenHashMap<>();
    Lock lock = this.knownItemsLock.readLock();
    lock.lock();
    try {
      for (ObjectCursor<ObjectSet<String>> idsCursor : knownItems.values()) {
        ObjectSet<String> ids = idsCursor.value;
        synchronized (ids) {
          for (ObjectCursor<String> idCursor : ids) {
            counts.addTo(idCursor.value, 1);
          }
        }
      }
    } finally {
      lock.unlock();
    }
    return counts;
  }

  void addKnownItems(String user, Collection<String> items) {
    ObjectSet<String> knownItemsForUser = getKnownItems(user);

    if (knownItemsForUser == null) {
      Lock writeLock = this.knownItemsLock.writeLock();
      writeLock.lock();
      try {
        // Check again
        knownItemsForUser = this.knownItems.get(user);
        if (knownItemsForUser == null) {
          knownItemsForUser = new ObjectOpenHashSet<>();
          this.knownItems.put(user, knownItemsForUser);
        }
      } finally {
        writeLock.unlock();
      }
    }

    synchronized (knownItemsForUser) {
      for (String item : items) {
        knownItemsForUser.add(item);
      }
    }
  }

  public List<Pair<String,float[]>> getKnownItemVectorsForUser(String user) {
    float[] userVector = getUserVector(user);
    if (userVector == null) {
      return null;
    }
    ObjectSet<String> knownItems = getKnownItems(user);
    if (knownItems == null || knownItems.isEmpty()) {
      return null;
    }
    List<Pair<String,float[]>> idVectors = new ArrayList<>(knownItems.size());
    Lock lock = yLock.readLock();
    lock.lock();
    try {
      synchronized (knownItems) {
        for (ObjectCursor<String> knownItem : knownItems) {
          String itemID = knownItem.value;
          idVectors.add(new Pair<>(itemID, Y.get(itemID)));
        }
      }
    } finally {
      lock.unlock();
    }
    return idVectors;
  }

  public List<Pair<String,Double>> topDotWithUserVector(String user,
                                                        int howMany,
                                                        boolean considerKnownItems) {
    float[] userVector = getUserVector(user);
    if (userVector == null) {
      return null;
    }

    Iterable<ObjectObjectCursor<String,float[]>> entries = Y;

    if (!considerKnownItems) {
      ObjectSet<String> knownItems = getKnownItems(user);
      if (knownItems != null && !knownItems.isEmpty()) {
        entries = Iterables.filter(entries, new NotKnownPredicate(knownItems));
      }
    }

    Iterable<Pair<String,Double>> idDots =
        Iterables.transform(entries, new DotsFunction(userVector));
    return topN(idDots, howMany);
  }

  public List<Pair<String,Double>> mostSimilarItems(List<String> itemsList, int howMany) {
    List<Pair<String,Double>> itemScoresList = new ArrayList<>(itemsList.size());
    Iterable<ObjectObjectCursor<String,float[]>> entries = Y;

    // TODO need lock among other things
    for (ObjectObjectCursor<String,float[]> entry : entries) {
      double total = 0.0;
      double entryNorm = VectorMath.norm(entry.value);
      for (String item : itemsList) {
        if (!item.equalsIgnoreCase(entry.key)) {
          float[] itemFeatures = getItemVector(item);
          double similarity = VectorMath.dot(entry.value, itemFeatures)/(entryNorm * VectorMath.norm(itemFeatures));
          total += similarity;
        }
      }
      double result = total / itemsList.size();
      Preconditions.checkState(!(Double.isInfinite(result) || Double.isNaN(result)), "Bad similarity value");
      itemScoresList.add(new Pair<>(entry.key, result));
    }
    return topN(itemScoresList, howMany);
  }

  private List<Pair<String,Double>> topN(Iterable<Pair<String,Double>> pairs, int howMany) {
    Ordering<Pair<?,Double>> ordering = Ordering.from(PairComparators.<Double>bySecond());
    Lock lock = yLock.readLock();
    lock.lock();
    try {
      return ordering.greatestOf(pairs, howMany);
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
  void pruneKnownItems(Set<String> items) {
    Lock lock = this.knownItemsLock.readLock();
    lock.lock();
    try {
      for (ObjectCursor<ObjectSet<String>> collectionObjectCursor : this.knownItems.values()) {
        ObjectSet<String> knownItemsForUser = collectionObjectCursor.value;
        synchronized (knownItemsForUser) {
          Iterator<ObjectCursor<String>> it = knownItemsForUser.iterator();
          while (it.hasNext()) {
            if (!items.contains(it.next().value)) {
              it.remove();
            }
          }
        }
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

  private static final class DotsFunction
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

  private static final class CosineSimilarityFunction
      implements Function<ObjectObjectCursor<String,float[]>,Pair<String,Double>> {
    private final float[] itemVector;
    private final double itemVectorNorm;
    CosineSimilarityFunction(float[] itemVector) {
      this.itemVector = itemVector;
      this.itemVectorNorm = VectorMath.norm(itemVector);
    }
    @Override
    public Pair<String,Double> apply(ObjectObjectCursor<String,float[]> itemIDVector) {
      float[] otherItemVector = itemIDVector.value;
      double cosineSimilarity =  VectorMath.dot(itemVector, otherItemVector) /
          (itemVectorNorm * VectorMath.norm(otherItemVector));
      return new Pair<>(itemIDVector.key, cosineSimilarity);
    }
  }

  private static final class NotKnownPredicate
      implements Predicate<ObjectObjectCursor<String,float[]>> {
    private final ObjectSet<String> knownItemsForUser;
    NotKnownPredicate(ObjectSet<String> knownItemsForUser) {
      this.knownItemsForUser = knownItemsForUser;
    }
    @Override
    public boolean apply(ObjectObjectCursor<String,float[]> input) {
      synchronized (knownItemsForUser) {
        return !knownItemsForUser.contains(input.key);
      }
    }
  }

}
