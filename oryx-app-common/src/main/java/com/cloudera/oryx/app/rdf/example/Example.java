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

package com.cloudera.oryx.app.rdf.example;

import java.io.Serializable;
import java.util.Arrays;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

/**
 * Encapsulates one example, or data point: a set of features that predict a target feature.
 * Some features may be missing, and in the case of test examples, the target value may be
 * missing or unknown.
 */
public final class Example implements Serializable {

  private static final HashFunction HASH = Hashing.goodFastHash(32);

  private final Feature[] features;
  private final Feature target;
  private final int cachedHashCode;

  public Example(Feature target, Feature... features) {
    Preconditions.checkArgument(features != null);
    this.features = features;
    this.target = target;
    Hasher hasher = HASH.newHasher();
    for (Feature feature : features) {
      if (feature != null) {
        hasher.putInt(feature.hashCode());
      }
    }
    if (target != null) {
      hasher.putInt(target.hashCode());
    }
    cachedHashCode = hasher.hashCode();
  }
  
  public Feature getFeature(int i) {
    return features[i];
  }

  public Feature getTarget() {
    return target;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof Example)) {
      return false;
    }
    Example other = (Example) o;
    return Arrays.equals(features, other.features) && Objects.equal(target, other.target);
  }

  @Override
  public int hashCode() {
    return cachedHashCode;
  }

  @Override
  public String toString() {
    return target == null ? Arrays.toString(features) : Arrays.toString(features) + " -> " + target;
  }
  
}
