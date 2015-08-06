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

import net.openhft.koloboke.collect.map.ObjObjMap;
import net.openhft.koloboke.collect.map.hash.HashObjObjMaps;
import net.openhft.koloboke.collect.set.ObjSet;
import net.openhft.koloboke.collect.set.hash.HashObjSets;
import net.openhft.koloboke.function.BiConsumer;
import org.apache.commons.math3.linear.RealMatrix;

import com.cloudera.oryx.common.collection.KeyOnlyBiPredicate;
import com.cloudera.oryx.common.collection.NotContainsPredicate;
import com.cloudera.oryx.common.collection.Predicates;
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

  public int size() {
    return vectors.size();
  }

  public float[] getVector(String id) {
    try (AutoLock al = lock.autoReadLock()) {
      return vectors.get(id);
    }
  }

  public void setVector(String id, float[] vector) {
    try (AutoLock al = lock.autoWriteLock()) {
      if (vectors.put(id, vector) == null) {
        // ID was actually new
        recentIDs.add(id);
      }
    }
  }

  public void removeVector(String id) {
    try (AutoLock al = lock.autoWriteLock()) {
      vectors.remove(id);
      recentIDs.remove(id);
    }
  }

  public void addAllIDsTo(Collection<String> allIDs) {
    try (AutoLock al = lock.autoReadLock()) {
      allIDs.addAll(vectors.keySet());
    }
  }

  public void addAllRecentTo(Collection<String> allRecent) {
    try (AutoLock al = lock.autoReadLock()) {
      allRecent.addAll(recentIDs);
    }
  }

  public void prune(Collection<String> newModelIDs) {
    // Keep all IDs in the new model, or, that have been added since last model
    try (AutoLock al = lock.autoWriteLock()) {
      vectors.removeIf(new KeyOnlyBiPredicate<>(Predicates.and(
          new NotContainsPredicate<>(newModelIDs), new NotContainsPredicate<>(recentIDs))));
      recentIDs.clear();
    }
  }

  public void forEach(BiConsumer<String,float[]> action) {
    try (AutoLock al = lock.autoReadLock()) {
      vectors.forEach(action);
    }
  }

  public RealMatrix getVTV() {
    try (AutoLock al = lock.autoReadLock()) {
      return VectorMath.transposeTimesSelf(vectors.values());
    }
  }

}
