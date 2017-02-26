/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
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

package com.cloudera.oryx.app.classreg.example;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Preconditions;

/**
 * Represents a value of a categorical feature -- one that takes on discrete, unordered values like
 * {@code {male,female}} rather than a continuous or discrete numeric value.
 *
 * @see NumericFeature
 */
public final class CategoricalFeature implements Feature {

  // Not obvious we can get away with a cache without weak entries, but there should be relatively
  // few distinct category values ever observed
  private static final Map<Integer,CategoricalFeature> FEATURE_CACHE = new ConcurrentHashMap<>();

  private final int encoding;

  private CategoricalFeature(int encoding) {
    this.encoding = encoding;
  }

  /**
   * @param encoding category value ID to create {@code CategoricalFeature} for
   * @return {@code CategoricalFeature} representing the category value specified by ID
   */
  public static CategoricalFeature forEncoding(int encoding) {
    Preconditions.checkArgument(encoding >= 0);
    // Not important if several threads get here
    return FEATURE_CACHE.computeIfAbsent(encoding, k -> new CategoricalFeature(encoding));
  }

  /**
   * @return category value ID represented by this {@code CategoricalFeature}
   */
  public int getEncoding() {
    return encoding;
  }

  /**
   * @return {@link FeatureType#CATEGORICAL}
   */
  @Override
  public FeatureType getFeatureType() {
    return FeatureType.CATEGORICAL;
  }

  @Override
  public String toString() {
    return ":" + encoding;
  }
  
  @Override
  public int hashCode() {
    return encoding;
  }
  
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof CategoricalFeature)) {
      return false;
    }
    CategoricalFeature other = (CategoricalFeature) o;
    return encoding == other.encoding;
  }

}
