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
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.koloboke.collect.ObjCursor;
import com.koloboke.collect.map.ObjIntMap;
import com.koloboke.collect.map.ObjObjMap;
import com.koloboke.collect.map.hash.HashObjIntMaps;
import com.koloboke.collect.map.hash.HashObjObjMaps;
import com.koloboke.collect.set.ObjSet;
import com.koloboke.collect.set.hash.HashObjSets;
import com.koloboke.function.ObjDoubleToDoubleFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.api.serving.ServingModel;
import com.cloudera.oryx.app.als.FeatureVectorsPartition;
import com.cloudera.oryx.app.als.PartitionedFeatureVectors;
import com.cloudera.oryx.app.als.RescorerProvider;
import com.cloudera.oryx.app.als.SolverCache;
import com.cloudera.oryx.app.serving.als.CosineDistanceSensitiveFunction;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.collection.Pairs;
import com.cloudera.oryx.common.lang.AutoLock;
import com.cloudera.oryx.common.lang.AutoReadWriteLock;
import com.cloudera.oryx.common.math.Solver;

/**
 * Contains all data structures needed to serve real-time requests for an ALS-based recommender.
 */
public final class ALSServingModel implements ServingModel {

  private static final Logger log = LoggerFactory.getLogger(ALSServingModel.class);

  private static final ExecutorService executor = Executors.newFixedThreadPool(
      Runtime.getRuntime().availableProcessors(),
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ALSServingModel-%d").build());

  private final LocalitySensitiveHash lsh;
  /** User-feature matrix. */
  private final FeatureVectorsPartition X;
  /** Item-feature matrix. This is partitioned into several maps for parallel access. */
  private final PartitionedFeatureVectors Y;
  /** Remembers items that each user has interacted with*/
  private final ObjObjMap<String,ObjSet<String>> knownItems; // Right now no corresponding "knownUsers" object
  private final AutoReadWriteLock knownItemsLock;
  private final ObjSet<String> expectedUserIDs;
  private final AutoReadWriteLock expectedUserIDsLock;
  private final ObjSet<String> expectedItemIDs;
  private final AutoReadWriteLock expectedItemIDsLock;
  private final SolverCache cachedYTYSolver;
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

    X = new FeatureVectorsPartition();
    Y = new PartitionedFeatureVectors(
        lsh.getNumPartitions(),
        executor,
        (String id, float[] vector) -> lsh.getIndexFor(vector));

    knownItems = HashObjObjMaps.newMutableMap();
    knownItemsLock = new AutoReadWriteLock();

    expectedUserIDs = HashObjSets.newMutableSet();
    expectedUserIDsLock = new AutoReadWriteLock();
    expectedItemIDs = HashObjSets.newMutableSet();
    expectedItemIDsLock = new AutoReadWriteLock();

    cachedYTYSolver = new SolverCache(executor, Y);

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
    return Y.getVector(item);
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
    Y.setVector(item, vector);
    try (AutoLock al = expectedItemIDsLock.autoWriteLock()) {
      expectedItemIDs.remove(item);
    }
    // Not clear if it's too inefficient to clear and recompute YtY solver every time any bit
    // of Y changes, but it's the most correct
    cachedYTYSolver.setDirty();
  }

  /**
   * @param user user to get known items for
   * @return set of known items for the user (immutable, but thread-safe)
   */
  public Set<String> getKnownItems(String user) {
    ObjSet<String> knownItems = doGetKnownItems(user);
    if (knownItems == null) {
      return Collections.emptySet();
    }
    synchronized (knownItems) {
      if (knownItems.isEmpty()) {
        return Collections.emptySet();
      }
      // Must copy since the original object is synchronized
      return HashObjSets.newImmutableSet(knownItems);
    }
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
      knownItems.forEach((userID, ids) -> {
        int numItems;
        synchronized (ids) {
          numItems = ids.size();
        }
        counts.addValue(userID, numItems);
      });
    }
    return counts;
  }

  /**
   * @return mapping of item IDs to count of users that have interacted with that item
   */
  public Map<String,Integer> getItemCounts() {
    ObjIntMap<String> counts = HashObjIntMaps.newUpdatableMap();
    try (AutoLock al = knownItemsLock.autoReadLock()) {
      knownItems.values().forEach(ids -> {
        synchronized (ids) {
          ids.forEach(id -> counts.addValue(id, 1));
        }
      });
    }
    return counts;
  }

  void addKnownItems(String user, Collection<String> items) {
    if (!items.isEmpty()) {
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
    Collection<String> knownItems = doGetKnownItems(user);
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

  public Stream<Pair<String,Double>> topN(
      CosineDistanceSensitiveFunction scoreFn,
      ObjDoubleToDoubleFunction<String> rescoreFn,
      int howMany,
      Predicate<String> allowedPredicate) {
    int[] candidateIndices = lsh.getCandidateIndices(scoreFn.getTargetVector());
    Stream<Pair<String,Double>> stream = Y.mapPartitionsParallel(
        partition -> {
          TopNConsumer consumer = new TopNConsumer(howMany, scoreFn, rescoreFn, allowedPredicate);
          partition.forEach(consumer);
          return consumer.getTopN();
        },
        candidateIndices,
        false);
    return stream.sorted(Pairs.orderBySecond(Pairs.SortOrder.DESCENDING)).limit(howMany);
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
    Y.addAllIDsTo(allItemIDs);
    return allItemIDs;
  }

  /**
   * @return a {@link Solver} for use in solving systems involving YT*Y
   */
  public Solver getYTYSolver() {
    return cachedYTYSolver.get(true);
  }

  void precomputeSolvers() {
    cachedYTYSolver.compute();
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
    Y.retainRecentAndIDs(items);
    try (AutoLock al = expectedItemIDsLock.autoWriteLock()) {
      expectedItemIDs.clear();
      expectedItemIDs.addAll(items);
      Y.removeAllIDsFrom(expectedItemIDs);
    }
  }

  /**
   * Like {@link #retainRecentAndUserIDs(Collection)} and {@link #retainRecentAndItemIDs(Collection)}
   * but affects the known-items data structure.
   *
   * @param users users that should be retained, which are coming in the new model updates
   * @param items items that should be retained, which are coming in the new model updates
   */
  void retainRecentAndKnownItems(Collection<String> users, Collection<String> items) {
    // Keep all users in the new model, or, that have been added since last model
    Collection<String> recentUserIDs = HashObjSets.newMutableSet();
    X.addAllRecentTo(recentUserIDs);
    try (AutoLock al = knownItemsLock.autoWriteLock()) {
      knownItems.removeIf((key, value) -> !users.contains(key) && !recentUserIDs.contains(key));
    }

    // This will be easier to quickly copy the whole (smallish) set rather than
    // deal with locks below
    Collection<String> allRecentKnownItems = HashObjSets.newMutableSet();
    Y.addAllRecentTo(allRecentKnownItems);

    Predicate<String> notKeptOrRecent = value -> !items.contains(value) && !allRecentKnownItems.contains(value);
    try (AutoLock al = knownItemsLock.autoReadLock()) {
      knownItems.values().forEach(knownItemsForUser -> {
        synchronized (knownItemsForUser) {
          // knownItemsForUser.removeIf(notKeptOrRecent);
          // TODO remove this temporary hack workaround and restore above
          // see https://github.com/OryxProject/oryx/issues/304
          ObjCursor<?> cursor = knownItemsForUser.cursor();
          while (cursor.moveNext()) {
            Object o = cursor.elem();
            if (!(o instanceof String)) {
              log.warn("Found non-String collection: {}", o);
              cursor.remove();
            } else if (notKeptOrRecent.test((String) o)) {
              cursor.remove();
            }
          }
        }
      });
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
    return Y.size();
  }

  @Override
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
    return "ALSServingModel[features:" + features + ", implicit:" + implicit +
        ", X:(" + getNumUsers() + " users), Y:(" + getNumItems() + " items, partitions: " +
        Y + "...), fractionLoaded:" + getFractionLoaded() + "]";
  }

}
