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

package com.cloudera.oryx.common.collection;

import java.util.Comparator;

/**
 * Provides utility implementations of {@link Comparator} for {@link Pair}s.
 */
public final class PairComparators {

  private PairComparators() {}

  public static <K extends Comparable<K>> Comparator<Pair<K,?>> byFirst() {
    return new Comparator<Pair<K,?>>() {
      @Override
      public int compare(Pair<K,?> p1, Pair<K,?> p2) {
        return p1.getFirst().compareTo(p2.getFirst());
      }
    };
  }

  public static <V extends Comparable<V>> Comparator<Pair<?,V>> bySecond() {
    return new Comparator<Pair<?,V>>() {
      @Override
      public int compare(Pair<?,V> p1, Pair<?,V> p2) {
        return p1.getSecond().compareTo(p2.getSecond());
      }
    };
  }

}
