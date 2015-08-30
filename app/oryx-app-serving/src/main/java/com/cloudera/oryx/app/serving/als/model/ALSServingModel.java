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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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

import com.cloudera.oryx.app.als.FeatureVectors;
import com.cloudera.oryx.app.als.RescorerProvider;
import com.cloudera.oryx.app.serving.als.CosineDistanceSensitiveFunction;
import com.cloudera.oryx.common.collection.KeyOnlyBiPredicate;
import com.cloudera.oryx.common.collection.NotContainsPredicate;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.collection.PairComparators;
import com.cloudera.oryx.common.collection.Predicates;
import com.cloudera.oryx.common.lang.AutoLock;
import com.cloudera.oryx.common.lang.AutoReadWriteLock;
import com.cloudera.oryx.common.lang.LoggingCallable;
import com.cloudera.oryx.common.math.LinearSystemSolver;
import com.cloudera.oryx.common.math.Solver;

/**
 * Contains all data structures needed to serve real-time requests for an ALS-based recommender.
 */
public final class ALSServingModel {

  /** Number of partitions for items data structures. */
  private static final ExecutorService executor = Executors.newFixedThreadPool(
      Runtime.getRuntime().availableProcessors(),
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ALSServingModel-%d").build());

  private final LocalitySensitiveHash lsh;
  /** User-feature matrix. */
  private final FeatureVectors X;
  /** Item-feature matrix. This is partitioned into several maps for parallel access. */
  private final FeatureVectors[] Y;
  /** Maps item IDs to their existing partition, if any */
  private final ObjIntMap<String> yPartitionMap;
  /** Controls access to yPartitionMap. */
  private final AutoReadWriteLock yPartitionMapLock;
  /** Remembers items that each user has interacted with*/
  private final ObjObjMap<String,ObjSet<String>> knownItems; // Right now no corresponding "knownUsers" object
  private final AutoReadWriteLock knownItemsLock;
  private final ObjSet<String> expectedUserIDs;
  private final AutoReadWriteLock expectedUserIDsLock;
  private final ObjSet<String> expectedItemIDs;
  private final AutoReadWriteLock expectedItemIDsLock;
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
  ALSServingModel(int features, boolean implicit, double sampleRate, RescorerProvider rescorerProvider) {
    Preconditions.checkArgument(features > 0);
    Preconditions.checkArgument(sampleRate > 0.0 && sampleRate <= 1.0);

    lsh = new LocalitySensitiveHash(sampleRate, features);

    X = new FeatureVectors();
    Y = new FeatureVectors[lsh.getNumPartitions()];
    for (int i = 0; i < Y.length; i++) {
      Y[i] = new FeatureVectors();
    }
    yPartitionMap = HashObjIntMaps.newMutableMap();
    yPartitionMapLock = new AutoReadWriteLock();

    knownItems = HashObjObjMaps.newMutableMap();
    knownItemsLock = new AutoReadWriteLock();

    expectedUserIDs = HashObjSets.newMutableSet();
    expectedUserIDsLock = new AutoReadWriteLock();
    expectedItemIDs = HashObjSets.newMutableSet();
    expectedItemIDsLock = new AutoReadWriteLock();

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
    return X.getVector(user);
  }

  public float[] getItemVector(String item) {
    int partition;
    try (AutoLock al = yPartitionMapLock.autoReadLock()) {
      partition = yPartitionMap.getOrDefault(item, Integer.MIN_VALUE);
    }
    if (partition < 0) {
      return null;
    }
    return Y[partition].getVector(item);
  }

  void setUserVector(String user, float[] vector) {
    Preconditions.checkArgument(vector.length == features);
    X.setVector(user, vector);
    try (AutoLock al = expectedUserIDsLock.autoWriteLock()) {
      expectedUserIDs.remove(user);
    }
  }

  void setItemVector(String item, float[] vector) {
    Preconditions.checkArgument(vector.length == features);
    int newPartition = lsh.getIndexFor(vector);
    // Exclusive update to mapping -- careful since other locks are acquired inside here
    try (AutoLock al = yPartitionMapLock.autoWriteLock()) {
      int existingPartition = yPartitionMap.getOrDefault(item, Integer.MIN_VALUE);
      if (existingPartition >= 0 && existingPartition != newPartition) {
        // Move from one to the other partition, so first remove old entry
        Y[existingPartition].removeVector(item);
        // Note that it's conceivable that a recommendation call sees *no* copy of this
        // item here in this brief window
      }
      // Then regardless put in new partition
      Y[newPartition].setVector(item, vector);
      yPartitionMap.put(item, newPartition);
    }
    try (AutoLock al = expectedItemIDsLock.autoWriteLock()) {
      expectedItemIDs.remove(item);
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
    try (AutoLock al = knownItemsLock.autoReadLock()) {
      return knownItems.get(user);
    }
  }

  /**
   * @return mapping of user IDs to count of items the user has interacted with
   */
  public Map<String,Integer> getUserCounts() {
    ObjIntMap<String> counts = HashObjIntMaps.newUpdatableMap();
    try (AutoLock al = knownItemsLock.autoReadLock()) {
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
    try (AutoLock al = knownItemsLock.autoReadLock()) {
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
      try (AutoLock al = knownItemsLock.autoWriteLock()) {
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

  /**
   * @param user user to get known item vectors for
   * @return {@code null} if the user is not known to the model, or if there are no known items for the user
   */
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
        if (vector != null) {
          idVectors.add(new Pair<>(itemID, vector));
        }
      }
      return idVectors.isEmpty() ? null : idVectors;
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
      final FeatureVectors yPartition = Y[partition];
      if (yPartition.size() > 0) {
        tasks.add(new LoggingCallable<Iterable<Pair<String,Double>>>() {
          @Override
          public Iterable<Pair<String,Double>> doCall() {
            Queue<Pair<String,Double>> topN =
                new PriorityQueue<>(howMany + 1, PairComparators.<Double>bySecond());
            yPartition.forEach(new TopNConsumer(topN, howMany, scoreFn, rescoreFn, allowedPredicate));
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
    Collection<String> allUserIDs = HashObjSets.newMutableSet();
    X.addAllIDsTo(allUserIDs);
    return allUserIDs;
  }

  /**
   * @return all item IDs in the model
   */
  public Collection<String> getAllItemIDs() {
    Collection<String> allItemIDs = HashObjSets.newMutableSet();
    for (FeatureVectors yPartition : Y) {
      yPartition.addAllIDsTo(allItemIDs);
    }
    return allItemIDs;
  }

  public Solver getYTYSolver() {
    RealMatrix YTY = null;
    for (FeatureVectors yPartition : Y) {
      RealMatrix YTYpartial = yPartition.getVTV();
      if (YTYpartial != null) {
        YTY = YTY == null ? YTYpartial : YTY.add(YTYpartial);
      }
    }
    return LinearSystemSolver.getSolver(YTY);
  }

  /**
   * Retains only users that are expected to appear
   * in the upcoming model updates, or, that have arrived recently. This also clears the
   * recent known users data structure.
   *
   * @param users users that should be retained, which are coming in the new model updates
   */
  void retainRecentAndUserIDs(Collection<String> users) {
    X.retainRecentAndIDs(users);
    try (AutoLock al = expectedUserIDsLock.autoWriteLock()) {
      expectedUserIDs.clear();
      expectedUserIDs.addAll(users);
      X.removeAllIDsFrom(expectedUserIDs);
    }
  }

  /**
   * Retains only items that are expected to appear
   * in the upcoming model updates, or, that have arrived recently. This also clears the
   * recent known items data structure.
   *
   * @param items items that should be retained, which are coming in the new model updates
   */
  void retainRecentAndItemIDs(Collection<String> items) {
    for (FeatureVectors yPartition : Y) {
      yPartition.retainRecentAndIDs(items);
    }
    try (AutoLock al = expectedItemIDsLock.autoWriteLock()) {
      expectedItemIDs.clear();
      expectedItemIDs.addAll(items);
      for (FeatureVectors yPartition : Y) {
        yPartition.removeAllIDsFrom(expectedItemIDs);
      }
    }
  }

  /**
   * Like {@link #retainRecentAndUserIDs(Collection)} and {@link #retainRecentAndItemIDs(Collection)}
   * but affects the known-items data structure.
   *
   * @param users users that should be retained, which are coming in the new model updates
   * @param items items that should be retained, which are coming in the new model updates
   */
  void retainRecentAndKnownItems(Collection<String> users, final Collection<String> items) {
    // Keep all users in the new model, or, that have been added since last model
    Collection<String> recentUserIDs = HashObjSets.newMutableSet();
    X.addAllRecentTo(recentUserIDs);
    try (AutoLock al = knownItemsLock.autoWriteLock()) {
      knownItems.removeIf(new KeyOnlyBiPredicate<>(Predicates.and(
          new NotContainsPredicate<>(users), new NotContainsPredicate<>(recentUserIDs))));
    }

    // This will be easier to quickly copy the whole (smallish) set rather than
    // deal with locks below
    final Collection<String> allRecentKnownItems = HashObjSets.newMutableSet();
    for (FeatureVectors yPartition : Y) {
      yPartition.addAllRecentTo(allRecentKnownItems);
    }

    Predicate<String> notKeptOrRecent = new Predicate<String>() {
      @Override
      public boolean test(String value) {
        return !items.contains(value) && !allRecentKnownItems.contains(value);
      }
    };
    try (AutoLock al = knownItemsLock.autoReadLock()) {
      for (ObjSet<String> knownItemsForUser : knownItems.values()) {
        synchronized (knownItemsForUser) {
          knownItemsForUser.removeIf(notKeptOrRecent);
        }
      }
    }
  }

  /**
   * @return number of users in the model
   */
  public int getNumUsers() {
    return X.size();
  }

  /**
   * @return number of items in the model
   */
  public int getNumItems() {
    int total = 0;
    for (FeatureVectors yPartition : Y) {
      total += yPartition.size();
    }
    return total;
  }

  /**
   * @return fraction of IDs that were expected to be in the model whose value has been
   *  loaded from an update
   */
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
    float loaded = (float) getNumUsers() + getNumItems();
    return loaded / (loaded + expected);
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
        partitionSizes + "...), fractionLoaded:" + getFractionLoaded() + "]";
  }

}
