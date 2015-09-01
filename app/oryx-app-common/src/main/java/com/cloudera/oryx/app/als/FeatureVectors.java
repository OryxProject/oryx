/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
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

package com.cloudera.oryx.app.als;

import java.util.Collection;
import java.util.function.BiConsumer;

import net.openhft.koloboke.collect.map.ObjObjMap;
import net.openhft.koloboke.collect.map.hash.HashObjObjMaps;
import net.openhft.koloboke.collect.set.ObjSet;
import net.openhft.koloboke.collect.set.hash.HashObjSets;
import org.apache.commons.math3.linear.RealMatrix;

import com.cloudera.oryx.common.lang.AutoLock;
import com.cloudera.oryx.common.lang.AutoReadWriteLock;
import com.cloudera.oryx.common.math.VectorMath;

/**
 * Encapsulates a feature vectors, keyed by an ID, and associated data structures.
 * This supports the representation of user-feature and item-feature vectors in {@code ALSServingModel}
 * or {@code ALSSpeedModel}.
 */
public final class FeatureVectors {

  private final ObjObjMap<String,float[]> vectors;
  private final ObjSet<String> recentIDs;
  private final AutoReadWriteLock lock;

  public FeatureVectors() {
    vectors = HashObjObjMaps.newMutableMap();
    recentIDs = HashObjSets.newMutableSet();
    lock = new AutoReadWriteLock();
  }

  /**
   * @return number of IDs / feature vectors
   */
  public int size() {
    return vectors.size();
  }

  /**
   * @param id ID to get feature vector for
   * @return feature vector for ID, or {@code null} if doesn't exist
   */
  public float[] getVector(String id) {
    try (AutoLock al = lock.autoReadLock()) {
      return vectors.get(id);
    }
  }

  /**
   * Sets the value of a feature vector for an ID. If no feature vector previously existed,
   * it is considered a "recent" ID until the next {@link #retainRecentAndIDs(Collection)} call.
   *
   * @param id ID to set feature vector for
   * @param vector new feature vector
   */
  public void setVector(String id, float[] vector) {
    try (AutoLock al = lock.autoWriteLock()) {
      if (vectors.put(id, vector) == null) {
        // ID was actually new
        recentIDs.add(id);
      }
    }
  }

  /**
   * @param id ID to remove feature vector for
   */
  public void removeVector(String id) {
    try (AutoLock al = lock.autoWriteLock()) {
      vectors.remove(id);
      recentIDs.remove(id);
    }
  }

  /**
   * Adds all IDs that are mapped to a feature vector to a given collection
   *
   * @param allIDs collection to add IDs to
   */
  public void addAllIDsTo(Collection<String> allIDs) {
    try (AutoLock al = lock.autoReadLock()) {
      allIDs.addAll(vectors.keySet());
    }
  }

  /**
   * Removes all IDs that are mapped to a feature vector from a given collection
   *
   * @param allIDs collection to add IDs to
   */
  public void removeAllIDsFrom(Collection<String> allIDs) {
    try (AutoLock al = lock.autoReadLock()) {
      allIDs.removeAll(vectors.keySet());
    }
  }

  /**
   * Add all recently set IDs to the given collection
   *
   * @param allRecent collection to add IDs to
   */
  public void addAllRecentTo(Collection<String> allRecent) {
    try (AutoLock al = lock.autoReadLock()) {
      allRecent.addAll(recentIDs);
    }
  }

  /**
   * Given IDs that are part of a new model, and whose values are going to be sent later,
   * retain only IDs that are also in the new model (or have been recently seen -- possibly since
   * last model was built), and remove the rest.
   *
   * @param newModelIDs new model IDs
   */
  public void retainRecentAndIDs(Collection<String> newModelIDs) {
    try (AutoLock al = lock.autoWriteLock()) {
      vectors.removeIf((key, value) -> !newModelIDs.contains(key) && !recentIDs.contains(key));
      recentIDs.clear();
    }
  }

  /**
   * @param action function to apply to every ID/vector pair
   */
  public void forEach(BiConsumer<String,float[]> action) {
    try (AutoLock al = lock.autoReadLock()) {
      vectors.forEach(action);
    }
  }

  /**
   * @return considering the feature vectors as the rows of a matrix V, this computes V^T * V
   */
  public RealMatrix getVTV() {
    try (AutoLock al = lock.autoReadLock()) {
      return VectorMath.transposeTimesSelf(vectors.values());
    }
  }

  @Override
  public String toString() {
    return "FeatureVectors[size:" + size() + "]";
  }

}
