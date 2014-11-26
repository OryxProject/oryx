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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import net.openhft.koloboke.collect.map.ObjIntMap;
import net.openhft.koloboke.collect.map.ObjObjMap;
import net.openhft.koloboke.collect.map.hash.HashObjIntMaps;
import net.openhft.koloboke.collect.map.hash.HashObjObjMaps;
import net.openhft.koloboke.collect.set.ObjSet;
import net.openhft.koloboke.collect.set.hash.HashObjSets;
import net.openhft.koloboke.function.BiConsumer;
import net.openhft.koloboke.function.Predicate;
import org.apache.commons.math3.linear.RealMatrix;

import com.cloudera.oryx.common.collection.AndPredicate;
import com.cloudera.oryx.common.collection.KeyOnlyBiPredicate;
import com.cloudera.oryx.common.collection.NotContainsPredicate;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.collection.PairComparators;
import com.cloudera.oryx.common.lang.LoggingCallable;
import com.cloudera.oryx.common.math.LinearSystemSolver;
import com.cloudera.oryx.common.math.Solver;
import com.cloudera.oryx.common.math.VectorMath;
import com.cloudera.oryx.ml.serving.als.DoubleFunction;

/**
 * Contains all data structures needed to serve real-time requests for an ALS-based recommender.
 */
public final class ALSServingModel {

  /** Number of partitions for items data structures. */
  private static final int PARTITIONS = Runtime.getRuntime().availableProcessors();
  // PARTITIONS == 1 is supported mostly for testing now
  private static final ExecutorService executor = PARTITIONS <= 1 ? null :
      Executors.newFixedThreadPool(PARTITIONS,
          new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ALSServingModel-%d").build());

  /** User-feature matrix, where row is keyed by user ID string and row is a dense float array. */
  private final ObjObjMap<String,float[]> X;
  /**
   * Item-feature matrix, where row is keyed by item ID string and row is a dense float array.
   * This is partitioned into several maps for parallel access.
   */
  private final ObjObjMap<String,float[]>[] Y;
  /** Remembers user IDs added since last model. */
  private final Collection<String> recentNewUsers;
  /** Remembers item IDs added since last model. Partitioned like Y. */
  private final Collection<String>[] recentNewItems;
  /** Remembers items that each user has interacted with*/
  private final ObjObjMap<String,ObjSet<String>> knownItems;
  // Right now no corresponding "knownUsers" object
  /** Controls access to X, knownItems, and recentNewUsers. */
  private final ReadWriteLock xLock;
  /** Controls access to partitions of Y, and is also used to control access to recentNewItems. */
  private final ReadWriteLock[] yLocks;
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
  @SuppressWarnings("unchecked")
  ALSServingModel(int features, boolean implicit) {
    Preconditions.checkArgument(features > 0);

    X = HashObjObjMaps.newMutableMap();
    Y = (ObjObjMap<String,float[]>[]) Array.newInstance(ObjObjMap.class, PARTITIONS);
    for (int i = 0; i < Y.length; i++) {
      Y[i] = HashObjObjMaps.newMutableMap();
    }

    recentNewUsers = new HashSet<>();
    recentNewItems = (Collection<String>[]) Array.newInstance(HashSet.class, PARTITIONS);
    for (int i = 0; i < recentNewItems.length; i++) {
      recentNewItems[i] = new HashSet<>();
    }

    knownItems = HashObjObjMaps.newMutableMap();

    xLock = new ReentrantReadWriteLock();
    yLocks = new ReentrantReadWriteLock[Y.length];
    for (int i = 0; i < yLocks.length; i++) {
      yLocks[i] = new ReentrantReadWriteLock();
    }

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
      if (X.put(user, vector) == null) {
        // User was actually new
        recentNewUsers.add(user);
      }
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
      if (Y[partition].put(item, vector) == null) {
        // Item was actually new
        recentNewItems[partition].add(item);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * @param user user to get known items for
   * @return set of known items for the user. Note that this object is not thread-safe and
   *  access must be {@code synchronized}
   */
  public Collection<String> getKnownItems(String user) {
    return doGetKnownItems(user);
  }

  private ObjSet<String> doGetKnownItems(String user) {
    Lock lock = xLock.readLock();
    lock.lock();
    try {
      return knownItems.get(user);
    } finally {
      lock.unlock();
    }
  }

  public Map<String,Integer> getItemCounts() {
    ObjIntMap<String> counts = HashObjIntMaps.newUpdatableMap();
    Lock lock = xLock.readLock();
    lock.lock();
    try {
      for (Collection<String> ids : knownItems.values()) {
        synchronized (ids) {
          for (String id : ids) {
            counts.addValue(id, 1);
          }
        }
      }
    } finally {
      lock.unlock();
    }
    return counts;
  }

  void addKnownItems(String user, Collection<String> items) {
    ObjSet<String> knownItemsForUser = doGetKnownItems(user);

    if (knownItemsForUser == null) {
      Lock writeLock = xLock.writeLock();
      writeLock.lock();
      try {
        // Check again
        knownItemsForUser = knownItems.get(user);
        if (knownItemsForUser == null) {
          knownItemsForUser = HashObjSets.newMutableSet();
          knownItems.put(user, knownItemsForUser);
        }
      } finally {
        writeLock.unlock();
      }
    }

    synchronized (knownItemsForUser) {
      knownItemsForUser.addAll(items);
    }
  }

  public List<Pair<String,float[]>> getKnownItemVectorsForUser(String user) {
    float[] userVector = getUserVector(user);
    if (userVector == null) {
      return null;
    }
    Collection<String> knownItems = getKnownItems(user);
    if (knownItems == null || knownItems.isEmpty()) {
      return null;
    }
    List<Pair<String,float[]>> idVectors = new ArrayList<>(knownItems.size());
    synchronized (knownItems) {
      for (String itemID : knownItems) {
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

          BiConsumer<String,float[]> topNProc = new BiConsumer<String,float[]>() {
            @Override
            public void accept(String key, float[] value) {
              if (allowedPredicate == null || allowedPredicate.test(key)) {
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
        itemsList.addAll(Y[partition].keySet());
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
   * Prunes the set of users in the model, by retaining only users that are expected to appear
   * in the upcoming model updates, or, that have arrived recently. This also clears the
   * recent known users data structure.
   *
   * @param users users that should be retained, which are coming in the new model updates
   */
  void pruneX(Collection<String> users) {
    // Keep all users in the new model, or, that have been added since last model
    Lock lock = xLock.writeLock();
    lock.lock();
    try {
      X.removeIf(new KeyOnlyBiPredicate<>(new AndPredicate<>(
          new NotContainsPredicate<>(users), new NotContainsPredicate<>(recentNewUsers))));
      recentNewUsers.clear();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Prunes the set of items in the model, by retaining only items that are expected to appear
   * in the upcoming model updates, or, that have arrived recently. This also clears the
   * recent known items data structure.
   *
   * @param items items that should be retained, which are coming in the new model updates
   */
  void pruneY(Collection<String> items) {
    for (int partition = 0; partition < Y.length; partition++) {
      // Keep all items in the new model, or, that have been added since last model
      Lock lock = yLocks[partition].writeLock();
      lock.lock();
      try {
        Y[partition].removeIf(new KeyOnlyBiPredicate<>(new AndPredicate<>(
            new NotContainsPredicate<>(items),
            new NotContainsPredicate<>(recentNewItems[partition]))));
        recentNewItems[partition].clear();
      } finally {
        lock.unlock();
      }
    }
  }

  /**
   * Like {@link #pruneX(Collection)} and {@link #pruneY(Collection)} but prunes the
   * known-items data structure.
   */
  void pruneKnownItems(Collection<String> users, final Collection<String> items) {
    // Keep all users in the new model, or, that have been added since last model
    Lock xWriteLock = xLock.writeLock();
    xWriteLock.lock();
    try {
      knownItems.removeIf(new KeyOnlyBiPredicate<>(new AndPredicate<>(
          new NotContainsPredicate<>(users), new NotContainsPredicate<>(recentNewUsers))));
    } finally {
      xWriteLock.unlock();
    }

    // This will be easier to quickly copy the whole (smallish) set rather than
    // deal with locks below
    final Collection<String> allRecentKnownItems = new HashSet<>();
    for (int partition = 0; partition < Y.length; partition++) {
      Lock yWriteLock = yLocks[partition].writeLock();
      yWriteLock.lock();
      try {
        allRecentKnownItems.addAll(recentNewItems[partition]);
      } finally {
        yWriteLock.unlock();
      }
    }

    Lock xReadLock = xLock.readLock();
    xReadLock.lock();
    try {
      for (ObjSet<String> knownItemsForUser : knownItems.values()) {
        synchronized (knownItemsForUser) {
          knownItemsForUser.removeIf(new Predicate<String>() {
            @Override
            public boolean test(String value) {
              return !items.contains(value) && !allRecentKnownItems.contains(value);
            }
          });
        }
      }
    } finally {
      xReadLock.unlock();
    }
  }

  @Override
  public String toString() {
    int numItems = 0;
    for (Map<?,?> partition : Y) {
      numItems += partition.size();
    }
    return "ALSServingModel[features:" + features + ", implicit:" + implicit +
        ", X:(" + X.size() + " users), Y:(" + numItems + " items)]";
  }

}
