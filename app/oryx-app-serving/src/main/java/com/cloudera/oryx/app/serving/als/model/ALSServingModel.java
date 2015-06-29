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

package com.cloudera.oryx.app.serving.als.model;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
import net.openhft.koloboke.function.ObjDoubleToDoubleFunction;
import net.openhft.koloboke.function.Predicate;
import org.apache.commons.math3.linear.RealMatrix;

import com.cloudera.oryx.app.als.RescorerProvider;
import com.cloudera.oryx.app.serving.als.CosineDistanceSensitiveFunction;
import com.cloudera.oryx.common.collection.KeyOnlyBiPredicate;
import com.cloudera.oryx.common.collection.NotContainsPredicate;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.collection.PairComparators;
import com.cloudera.oryx.common.collection.Predicates;
import com.cloudera.oryx.common.lang.AutoLock;
import com.cloudera.oryx.common.lang.LoggingCallable;
import com.cloudera.oryx.common.math.LinearSystemSolver;
import com.cloudera.oryx.common.math.Solver;
import com.cloudera.oryx.common.math.VectorMath;

/**
 * Contains all data structures needed to serve real-time requests for an ALS-based recommender.
 */
public final class ALSServingModel {

  /** Number of partitions for items data structures. */
  private static final ExecutorService executor = Executors.newFixedThreadPool(
      Runtime.getRuntime().availableProcessors(),
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ALSServingModel-%d").build());

  private final LocalitySensitiveHash lsh;
  /** User-feature matrix, where row is keyed by user ID string and row is a dense float array. */
  private final ObjObjMap<String,float[]> X;
  /**
   * Item-feature matrix, where row is keyed by item ID string and row is a dense float array.
   * This is partitioned into several maps for parallel access.
   */
  private final ObjObjMap<String,float[]>[] Y;
  /** Maps item IDs to their existing partition, if any */
  private final ObjIntMap<String> yPartitionMap;
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
  /** Controls access to yPartitionMap. */
  private final ReadWriteLock yPartitionMapLock;
  /** Number of features used in the model. */
  private final int features;
  /** Whether model uses implicit feedback. */
  private final boolean implicit;
  private final RescorerProvider rescorerProvider;

  /**
   * Creates an empty model.
   *
   * @param features number of features expected for user/item feature vectors
   * @param implicit whether model implements implicit feedback
   * @param sampleRate consider only approximately this fraction of all items when making recommendations.
   *  Candidates are chosen intelligently with locality sensitive hashing.
   * @param rescorerProvider optional instance of a {@link RescorerProvider}
   */
  @SuppressWarnings("unchecked")
  ALSServingModel(int features, boolean implicit, double sampleRate, RescorerProvider rescorerProvider) {
    Preconditions.checkArgument(features > 0);
    Preconditions.checkArgument(sampleRate > 0.0 && sampleRate <= 1.0);

    lsh = new LocalitySensitiveHash(sampleRate, features);

    X = HashObjObjMaps.newMutableMap();
    Y = (ObjObjMap<String,float[]>[]) Array.newInstance(ObjObjMap.class, lsh.getNumPartitions());
    for (int i = 0; i < Y.length; i++) {
      Y[i] = HashObjObjMaps.newMutableMap();
    }
    yPartitionMap = HashObjIntMaps.newMutableMap();

    recentNewUsers = new HashSet<>();
    recentNewItems = (Collection<String>[]) Array.newInstance(HashSet.class, lsh.getNumPartitions());
    for (int i = 0; i < recentNewItems.length; i++) {
      recentNewItems[i] = new HashSet<>();
    }

    knownItems = HashObjObjMaps.newMutableMap();

    xLock = new ReentrantReadWriteLock();
    yLocks = new ReentrantReadWriteLock[Y.length];
    for (int i = 0; i < yLocks.length; i++) {
      yLocks[i] = new ReentrantReadWriteLock();
    }
    yPartitionMapLock = new ReentrantReadWriteLock();

    this.features = features;
    this.implicit = implicit;
    this.rescorerProvider = rescorerProvider;
  }

  public int getFeatures() {
    return features;
  }

  public boolean isImplicit() {
    return implicit;
  }

  public RescorerProvider getRescorerProvider() {
    return rescorerProvider;
  }

  public float[] getUserVector(String user) {
    try (AutoLock al = new AutoLock(xLock.readLock())) {
      return X.get(user);
    }
  }

  public float[] getItemVector(String item) {
    int partition;
    try (AutoLock al = new AutoLock(yPartitionMapLock.readLock())) {
      partition = yPartitionMap.getOrDefault(item, Integer.MIN_VALUE);
    }
    if (partition < 0) {
      return null;
    }
    try (AutoLock al = new AutoLock(yLocks[partition].readLock())) {
      return Y[partition].get(item);
    }
  }

  void setUserVector(String user, float[] vector) {
    Objects.requireNonNull(vector);
    Preconditions.checkArgument(vector.length == features);
    try (AutoLock al = new AutoLock(xLock.writeLock())) {
      if (X.put(user, vector) == null) {
        // User was actually new
        recentNewUsers.add(user);
      }
    }
  }

  void setItemVector(String item, float[] vector) {
    Objects.requireNonNull(vector);
    Preconditions.checkArgument(vector.length == features);
    int newPartition = lsh.getIndexFor(vector);
    // Exclusive update to mapping -- careful since other locks are acquired inside here
    try (AutoLock al = new AutoLock(yPartitionMapLock.writeLock())) {
      int existingPartition = yPartitionMap.getOrDefault(item, Integer.MIN_VALUE);
      if (existingPartition >= 0 && existingPartition != newPartition) {
        // Move from one to the other partition, so first remove old entry
        try (AutoLock al2 = new AutoLock(yLocks[existingPartition].writeLock())) {
          Y[existingPartition].remove(item);
          // Not new, so no update to recentNewItems
        }
        // Note that it's conceivable that a recommendation call sees *no* copy of this
        // item here in this brief window
      }
      // Then regardless put in new partition
      try (AutoLock al2 = new AutoLock(yLocks[newPartition].writeLock())) {
        Y[newPartition].put(item, vector);
        // Consider it at least new to the partition if it moved partitions
        if (existingPartition != newPartition) {
          recentNewItems[newPartition].add(item);
        }
      }
      yPartitionMap.put(item, newPartition);
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
    try (AutoLock al = new AutoLock(xLock.readLock())) {
      return knownItems.get(user);
    }
  }

  /**
   * @return mapping of user IDs to count of items the user has interacted with
   */
  public Map<String,Integer> getUserCounts() {
    ObjIntMap<String> counts = HashObjIntMaps.newUpdatableMap();
    try (AutoLock al = new AutoLock(xLock.readLock())) {
      for (Map.Entry<String,ObjSet<String>> entry : knownItems.entrySet()) {
        String userID = entry.getKey();
        Collection<?> ids = entry.getValue();
        int numItems;
        synchronized (ids) {
          numItems = ids.size();
        }
        counts.addValue(userID, numItems);
      }
    }
    return counts;
  }

  /**
   * @return mapping of item IDs to count of users that have interacted with that item
   */
  public Map<String,Integer> getItemCounts() {
    ObjIntMap<String> counts = HashObjIntMaps.newUpdatableMap();
    try (AutoLock al = new AutoLock(xLock.readLock())) {
      for (Collection<String> ids : knownItems.values()) {
        synchronized (ids) {
          for (String id : ids) {
            counts.addValue(id, 1);
          }
        }
      }
    }
    return counts;
  }

  void addKnownItems(String user, Collection<String> items) {
    ObjSet<String> knownItemsForUser = doGetKnownItems(user);

    if (knownItemsForUser == null) {
      try (AutoLock al = new AutoLock(xLock.writeLock())) {
        // Check again
        knownItemsForUser = knownItems.get(user);
        if (knownItemsForUser == null) {
          knownItemsForUser = HashObjSets.newMutableSet();
          knownItems.put(user, knownItemsForUser);
        }
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
    if (knownItems == null) {
      return null;
    }
    synchronized (knownItems) {
      int size = knownItems.size();
      if (size == 0) {
        return null;
      }
      List<Pair<String,float[]>> idVectors = new ArrayList<>(size);
      for (String itemID : knownItems) {
        float[] vector = getItemVector(itemID);
        idVectors.add(new Pair<>(itemID, vector));
      }
      return idVectors;
    }
  }

  public List<Pair<String,Double>> topN(
      final CosineDistanceSensitiveFunction scoreFn,
      final ObjDoubleToDoubleFunction<String> rescoreFn,
      final int howMany,
      final Predicate<String> allowedPredicate) {

    int[] candidateIndices = lsh.getCandidateIndices(scoreFn.getTargetVector());
    List<Callable<Iterable<Pair<String,Double>>>> tasks = new ArrayList<>(candidateIndices.length);
    for (int partition : candidateIndices) {
      // Taking a liberty of reading size without lock as it should be a simple field read
      if (!Y[partition].isEmpty()) {
        final int thePartition = partition;
        tasks.add(new LoggingCallable<Iterable<Pair<String,Double>>>() {
          @Override
          public Iterable<Pair<String,Double>> doCall() {
            Queue<Pair<String,Double>> topN =
                new PriorityQueue<>(howMany + 1, PairComparators.<Double>bySecond());
            TopNConsumer topNProc =
                new TopNConsumer(topN, howMany, scoreFn, rescoreFn, allowedPredicate);

            try (AutoLock al = new AutoLock(yLocks[thePartition].readLock())) {
              Y[thePartition].forEach(topNProc);
            }
            // Ordering and excess items don't matter; will be merged and finally sorted later
            return topN;
          }
        });
      }
    }

    int numTasks = tasks.size();
    if (numTasks == 0) {
      return Collections.emptyList();
    }

    Ordering<Pair<?,Double>> ordering = Ordering.from(PairComparators.<Double>bySecond());
    if (numTasks == 1) {
      Iterable<Pair<String,Double>> iterable;
      try {
        iterable = tasks.get(0).call();
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
      return ordering.greatestOf(iterable, howMany);
    }

    List<Iterable<Pair<String,Double>>> iterables = new ArrayList<>(numTasks);
    try {
      for (Future<Iterable<Pair<String, Double>>> future : executor.invokeAll(tasks)) {
        iterables.add(future.get());
      }
    } catch (InterruptedException e) {
      throw new IllegalStateException(e);
    } catch (ExecutionException e) {
      throw new IllegalStateException(e.getCause());
    }
    return ordering.greatestOf(Iterables.concat(iterables), howMany);
  }

  /**
   * @return all user IDs in the model
   */
  public Collection<String> getAllUserIDs() {
    Collection<String> usersList;
    try (AutoLock al = new AutoLock(xLock.readLock())) {
      usersList = new ArrayList<>(X.keySet());
    }
    return usersList;
  }

  /**
   * @return all item IDs in the model
   */
  public Collection<String> getAllItemIDs() {
    Collection<String> itemsList = new ArrayList<>();
    for (int partition = 0; partition < Y.length; partition++) {
      try (AutoLock al = new AutoLock(yLocks[partition].readLock())) {
        itemsList.addAll(Y[partition].keySet());
      }
    }
    return itemsList;
  }

  public Solver getYTYSolver() {
    RealMatrix YTY = null;
    for (int partition = 0; partition < Y.length; partition++) {
      RealMatrix YTYpartial;
      try (AutoLock al = new AutoLock(yLocks[partition].readLock())) {
        YTYpartial = VectorMath.transposeTimesSelf(Y[partition].values());
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
    try (AutoLock al = new AutoLock(xLock.writeLock())) {
      X.removeIf(new KeyOnlyBiPredicate<>(Predicates.and(
          new NotContainsPredicate<>(users), new NotContainsPredicate<>(recentNewUsers))));
      recentNewUsers.clear();
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
      try (AutoLock al = new AutoLock(yLocks[partition].writeLock())) {
        Y[partition].removeIf(new KeyOnlyBiPredicate<>(Predicates.and(
            new NotContainsPredicate<>(items),
            new NotContainsPredicate<>(recentNewItems[partition]))));
        recentNewItems[partition].clear();
      }
    }
  }

  /**
   * Like {@link #pruneX(Collection)} and {@link #pruneY(Collection)} but prunes the
   * known-items data structure.
   */
  void pruneKnownItems(Collection<String> users, final Collection<String> items) {
    // Keep all users in the new model, or, that have been added since last model
    try (AutoLock al = new AutoLock(xLock.writeLock())) {
      knownItems.removeIf(new KeyOnlyBiPredicate<>(Predicates.and(
          new NotContainsPredicate<>(users), new NotContainsPredicate<>(recentNewUsers))));
    }

    // This will be easier to quickly copy the whole (smallish) set rather than
    // deal with locks below
    final Collection<String> allRecentKnownItems = new HashSet<>();
    for (int partition = 0; partition < Y.length; partition++) {
      try (AutoLock al = new AutoLock(yLocks[partition].writeLock())) {
        allRecentKnownItems.addAll(recentNewItems[partition]);
      }
    }

    try (AutoLock al = new AutoLock(xLock.readLock())) {
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
    }
  }

  /**
   * @return number of users in the model
   */
  public int getNumUsers() {
    // Reading size without lock
    return X.size();
  }

  /**
   * @return number of items in the model
   */
  public int getNumItems() {
    int total = 0;
    for (Map<?,?> partition : Y) {
      // Reading size without lock
      total += partition.size();
    }
    return total;
  }

  @Override
  public String toString() {
    int maxSize = 128;
    List<String> partitionSizes = new ArrayList<>(maxSize);
    for (int i = 0; i < Y.length; i++) {
      int size = Y[i].size();
      if (size > 0) {
        partitionSizes.add(i + ":" + size);
        if (partitionSizes.size() == maxSize) {
          partitionSizes.add("...");
          break;
        }
      }
    }
    return "ALSServingModel[features:" + features + ", implicit:" + implicit +
        ", X:(" + getNumUsers() + " users), Y:(" + getNumItems() + " items, partitions: " +
        partitionSizes + "...)]";
  }

}
