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

package com.cloudera.oryx.common.collection;

import java.io.Serializable;
import java.util.Objects;

/**
 * Encapsulates a pair of objects.
 *
 * @param <A> first object type
 * @param <B> second object type
 */
public final class Pair<A,B> implements Serializable {

  private final A first;
  private final B second;

  public Pair(A first, B second) {
    this.first = first;
    this.second = second;
  }

  public A getFirst() {
    return first;
  }

  public B getSecond() {
    return second;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(first) ^ Objects.hashCode(second);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof Pair)) {
      return false;
    }
    @SuppressWarnings("unchecked")
    Pair<A,B> other = (Pair<A,B>) o;
    return Objects.equals(first, other.first) && Objects.equals(second, other.second);
  }

  @Override
  public String toString() {
    return first + "," + second;
  }

}
