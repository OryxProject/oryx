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

/**
 * Encapsulates a feature vectors, keyed by an ID, and associated data structures.
 * This supports the representation of user-feature and item-feature vectors in {@code ALSServingModel}
 * or {@code ALSSpeedModel}.
 */
public interface FeatureVectors {

  /**
   * @return number of IDs / feature vectors
   */
  int size();

  /**
   * @param id ID to get feature vector for
   * @return feature vector for ID, or {@code null} if doesn't exist
   */
  float[] getVector(String id);

  /**
   * Sets the value of a feature vector for an ID. If no feature vector previously existed,
   * it is considered a "recent" ID until the next {@link #retainRecentAndIDs(Collection)} call.
   *
   * @param id ID to set feature vector for
   * @param vector new feature vector
   */
  void setVector(String id, float[] vector);

  /**
   * Adds all IDs that are mapped to a feature vector to a given collection
   *
   * @param allIDs collection to add IDs to
   */
  void addAllIDsTo(Collection<String> allIDs);

  /**
   * Removes all IDs that are mapped to a feature vector from a given collection
   *
   * @param allIDs collection to add IDs to
   */
  void removeAllIDsFrom(Collection<String> allIDs);

  /**
   * Add all recently set IDs to the given collection
   *
   * @param allRecent collection to add IDs to
   */
  void addAllRecentTo(Collection<String> allRecent);

  /**
   * Given IDs that are part of a new model, and whose values are going to be sent later,
   * retain only IDs that are also in the new model (or have been recently seen -- possibly since
   * last model was built), and remove the rest.
   *
   * @param newModelIDs new model IDs
   */
  void retainRecentAndIDs(Collection<String> newModelIDs);

  /**
   * @param background if true, a hint that this potentially expensive computation may be
   *  deprioritized and need not use all available resources to compute
   * @return considering the feature vectors as the rows of a matrix V, this computes V^T * V,
   *  as a dense lower-triangular matrix, represented in BLAS 'packed' form, which is
   *  column-major.
   */
  double[] getVTV(boolean background);

}
