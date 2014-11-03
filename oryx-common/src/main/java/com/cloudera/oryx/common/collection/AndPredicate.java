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

import com.carrotsearch.hppc.predicates.ObjectPredicate;

public final class AndPredicate<T> implements ObjectPredicate<T> {

  // Consider supporting arbitrary # later
  private final ObjectPredicate<T> a;
  private final ObjectPredicate<T> b;

  public AndPredicate(ObjectPredicate<T> a, ObjectPredicate<T> b) {
    this.a = a;
    this.b = b;
  }

  @Override
  public boolean apply(T value) {
    return a.apply(value) && b.apply(value);
  }

}
