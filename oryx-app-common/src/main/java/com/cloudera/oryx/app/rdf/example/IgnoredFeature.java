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

/**
 * Placeholder object for a feature value that is ignored in the model.
 */
public final class IgnoredFeature implements Feature {

  public static final IgnoredFeature INSTANCE = new IgnoredFeature();

  private IgnoredFeature() {
  }

  /**
   * @return {@link FeatureType#IGNORED}
   */
  @Override
  public FeatureType getFeatureType() {
    return FeatureType.IGNORED;
  }

  @Override
  public int hashCode() {
    return 0xDEADBEEF;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof IgnoredFeature;
  }

  @Override
  public String toString() {
    return "Ignored";
  }

}
