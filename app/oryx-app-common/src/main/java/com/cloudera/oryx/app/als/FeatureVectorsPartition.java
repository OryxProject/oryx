/*
 * Copyright (c) 2016, Cloudera, Inc. All Rights Reserved.
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

import com.koloboke.collect.map.ObjObjMap;
import com.koloboke.collect.map.hash.HashObjObjMaps;
import com.koloboke.collect.set.ObjSet;
import com.koloboke.collect.set.hash.HashObjSets;

import com.cloudera.oryx.common.lang.AutoLock;
import com.cloudera.oryx.common.lang.AutoReadWriteLock;
import com.cloudera.oryx.common.math.VectorMath;

/**
 * Encapsulates a feature vectors, keyed by an ID, and associated data structures.
 * This supports the representation of user-feature and item-feature vectors in {@code ALSServingModel}
 * or {@code ALSSpeedModel}. This represents one partition of vectors; see {@link PartitionedFeatureVectors}
 * for a representation of many partitions.
 */
public final class FeatureVectorsPartition implements FeatureVectors {

  private final ObjObjMap<String,float[]> vectors;
  private final ObjSet<String> recentIDs;
  private final AutoReadWriteLock lock;

  public FeatureVectorsPartition() {
    vectors = HashObjObjMaps.newMutableMap();
    recentIDs = HashObjSets.newMutableSet();
    lock = new AutoReadWriteLock();
  }

  @Override
  public int size() {
    return vectors.size();
  }

  @Override
  public float[] getVector(String id) {
    try (AutoLock al = lock.autoReadLock()) {
      return vectors.get(id);
    }
  }

  @Override
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
  void removeVector(String id) {
    try (AutoLock al = lock.autoWriteLock()) {
      vectors.remove(id);
      recentIDs.remove(id);
    }
  }

  @Override
  public void addAllIDsTo(Collection<String> allIDs) {
    try (AutoLock al = lock.autoReadLock()) {
      allIDs.addAll(vectors.keySet());
    }
  }

  @Override
  public void removeAllIDsFrom(Collection<String> allIDs) {
    try (AutoLock al = lock.autoReadLock()) {
      allIDs.removeAll(vectors.keySet());
    }
  }

  @Override
  public void addAllRecentTo(Collection<String> allRecent) {
    try (AutoLock al = lock.autoReadLock()) {
      allRecent.addAll(recentIDs);
    }
  }

  @Override
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

  @Override
  public double[] getVTV(boolean background) {
    // 'background' is ignored
    try (AutoLock al = lock.autoReadLock()) {
      return VectorMath.transposeTimesSelf(vectors.values());
    }
  }

  @Override
  public String toString() {
    return "FeatureVectors[size:" + size() + "]";
  }

}
