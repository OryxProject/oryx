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

package com.cloudera.oryx.common.collection;

import net.openhft.koloboke.function.Predicate;

/**
 * {@link Predicate}-related factory methods and utilities.
 */
public final class Predicates {

  private Predicates() {
  }

  /**
   * @param a first predicate
   * @param b second predicate
   * @param <T> operand
   * @return {@link Predicate} as logical AND of the two given predicates. If one is null, the other
   *  is returned
   */
  public static <T> Predicate<T> and(Predicate<T> a, Predicate<T> b) {
    if (a == null) {
      return b;
    }
    if (b == null) {
      return a;
    }
    return new AndPredicate<>(a, b);
  }

}
