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

package com.cloudera.oryx.common.collection;

import java.util.Comparator;

/**
 * {@link Pair}-related utilities.
 */
public final class Pairs {

  private Pairs() {}

  /**
   * @param order whether to sort ascending or descending; {@code null} comes last
   * @param <C> type of first element in {@link Pair}s to be compared
   * @param <D> type of second element in {@link Pair}s to be compared
   * @return an ordering on {@link Pair}s by first element as a {@link Comparator}
   */
  public static <C extends Comparable<C>,D> Comparator<Pair<C,D>> orderByFirst(SortOrder order) {
    Comparator<Pair<C,D>> ordering = Comparator.comparing(Pair::getFirst);
    if (order == SortOrder.DESCENDING) {
      ordering = ordering.reversed();
    }
    return Comparator.nullsLast(ordering);
  }

  /**
   * @param order whether to sort ascending or descending; {@code null} comes last
   * @param <C> type of first element in {@link Pair}s to be compared
   * @param <D> type of second element in {@link Pair}s to be compared
   * @return an ordering on {@link Pair}s by second element as a {@link Comparator}
   */
  public static <C,D extends Comparable<D>> Comparator<Pair<C,D>> orderBySecond(SortOrder order) {
    Comparator<Pair<C,D>> ordering = Comparator.comparing(Pair::getSecond);
    if (order == SortOrder.DESCENDING) {
      ordering = ordering.reversed();
    }
    return Comparator.nullsLast(ordering);
  }

  /**
   * Sort ordering for {@link Pair}s.
   */
  public enum SortOrder {
    ASCENDING,
    DESCENDING,
  }

}
