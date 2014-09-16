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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
import com.carrotsearch.hppc.predicates.ObjectPredicate;
import com.carrotsearch.hppc.procedures.ObjectObjectProcedure;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.math3.linear.RealMatrix;

import com.cloudera.oryx.common.collection.NotContainsPredicate;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.collection.PairComparators;
import com.cloudera.oryx.common.lang.LoggingCallable;
import com.cloudera.oryx.common.math.LinearSystemSolver;
import com.cloudera.oryx.common.math.Solver;
import com.cloudera.oryx.common.math.VectorMath;
import com.cloudera.oryx.ml.serving.als.DoubleFunction;

public final class ALSServingModel {

  private static final int PARTITIONS = Runtime.getRuntime().availableProcessors();
  // PARTITIONS == 1 is supported mostly for testing now
  private static final ExecutorService executor = PARTITIONS <= 1 ? null :
      Executors.newFixedThreadPool(PARTITIONS,
          new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ALSServingModel-%d").build());

  private final ObjectObjectMap<String,float[]> X;
  private final ObjectObjectMap<String,float[]>[] Y;
  private final ObjectObjectMap<String,ObjectSet<String>> knownItems;
  private final ReadWriteLock xLock;
  private final ReadWriteLock[] yLocks;
  private final ReadWriteLock knownItemsLock;
  private final int features;
  private final boolean implicit;

  ALSServingModel(int features, boolean implicit) {
    Preconditions.checkArgument(features > 0);
    X = new ObjectObjectOpenHashMap<>();
    @SuppressWarnings("unchecked")
    ObjectObjectMap<String,float[]>[] theY =
        (ObjectObjectMap<String,float[]>[]) Array.newInstance(ObjectObjectMap.class, PARTITIONS);
    for (int i = 0; i < theY.length; i++) {
      theY[i] = new ObjectObjectOpenHashMap<>();
    }
    Y = theY;
    knownItems = new ObjectObjectOpenHashMap<>();
    xLock = new ReentrantReadWriteLock();
    yLocks = new ReentrantReadWriteLock[Y.length];
    for (int i = 0; i < yLocks.length; i++) {
      yLocks[i] = new ReentrantReadWriteLock();
    }
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

  private static int partition(Object o) {
    return (o.hashCode() & 0x7FFFFFFF) % PARTITIONS;
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
    int partition = partition(item);
    Lock lock = yLocks[partition].readLock();
    lock.lock();
    try {
      return Y[partition].get(item);
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
    int partition = partition(item);
    Lock lock = yLocks[partition].writeLock();
    lock.lock();
    try {
      Y[partition].put(item, vector);
    } finally {
      lock.unlock();
    }
  }

  /**
   * @param user user to get known items for
   * @return set of known items for the user. Note that this object is not thread-safe and
   *  access must be {@code synchronized}
   */
  public ObjectSet<String> getKnownItems(String user) {
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
    synchronized (knownItems) {
      for (ObjectCursor<String> knownItem : knownItems) {
        String itemID = knownItem.value;
        int partition = partition(itemID);
        float[] vector;
        Lock lock = yLocks[partition].readLock();
        lock.lock();
        try {
          vector = Y[partition].get(itemID);
        } finally {
          lock.unlock();
        }
        idVectors.add(new Pair<>(itemID, vector));
      }
    }
    return idVectors;
  }

  public List<Pair<String,Double>> topN(
      final DoubleFunction<float[]> scoreFn,
      final int howMany,
      final Predicate<String> allowedPredicate) {

    List<Callable<Iterable<Pair<String, Double>>>> tasks = new ArrayList<>(Y.length);
    for (int partition = 0; partition < Y.length; partition++) {
      final int thePartition = partition;
      tasks.add(new LoggingCallable<Iterable<Pair<String,Double>>>() {
        @Override
        public Iterable<Pair<String,Double>> doCall() {
          final Queue<Pair<String,Double>> topN =
              new PriorityQueue<>(howMany + 1, PairComparators.<Double>bySecond());

          ObjectObjectProcedure<String,float[]> topNProc =
              new ObjectObjectProcedure<String,float[]>() {
                @Override
                public void apply(String key, float[] value) {
                  if (allowedPredicate == null || allowedPredicate.apply(key)) {
                    double score = scoreFn.apply(value);
                    if (topN.size() >= howMany) {
                      if (score > topN.peek().getSecond()) {
                        topN.poll();
                        topN.add(new Pair<>(key, score));
                      }
                    } else {
                      topN.add(new Pair<>(key, score));
                    }
                  }
                }
              };

          Lock lock = yLocks[thePartition].readLock();
          lock.lock();
          try {
            Y[thePartition].forEach(topNProc);
          } finally {
            lock.unlock();
          }
          // Ordering and excess items don't matter; will be merged and finally sorted later
          return topN;
        }
      });
    }

    List<Iterable<Pair<String, Double>>> iterables = new ArrayList<>();
    if (Y.length >= 2) {
      try {
        for (Future<Iterable<Pair<String, Double>>> future : executor.invokeAll(tasks)) {
          iterables.add(future.get());
        }
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      } catch (ExecutionException e) {
        throw new IllegalStateException(e.getCause());
      }
    } else {
      try {
        iterables.add(tasks.get(0).call());
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }

    return Ordering.from(PairComparators.<Double>bySecond())
        .greatestOf(Iterables.concat(iterables), howMany);
  }

  public Collection<String> getAllItemIDs() {
    Collection<String> itemsList = new ArrayList<>();
    for (int partition = 0; partition < Y.length; partition++) {
      Lock lock = yLocks[partition].readLock();
      lock.lock();
      try {
        for (ObjectCursor<String> intCursor : Y[partition].keys()) {
          itemsList.add(intCursor.value);
        }
      } finally {
        lock.unlock();
      }
    }
    return itemsList;
  }

  public Solver getYTYSolver() {
    RealMatrix YTY = null;
    for (int partition = 0; partition < Y.length; partition++) {
      RealMatrix YTYpartial;
      Lock lock = yLocks[partition].readLock();
      lock.lock();
      try {
        YTYpartial = VectorMath.transposeTimesSelf(Y[partition].values());
      } finally {
        lock.unlock();
      }
      if (YTYpartial != null) {
        YTY = YTY == null ? YTYpartial : YTY.add(YTYpartial);
      }
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
    ObjectPredicate<String> predicate = new NotContainsPredicate<>(items);
    for (int partition = 0; partition < Y.length; partition++) {
      Lock lock = yLocks[partition].writeLock();
      lock.lock();
      try {
        Y[partition].removeAll(predicate);
      } finally {
        lock.unlock();
      }
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
    return "ALSServingModel[features:" + features + ", implicit:" + implicit + "]";
  }

}
