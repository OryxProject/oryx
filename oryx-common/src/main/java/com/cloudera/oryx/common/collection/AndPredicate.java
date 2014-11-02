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
import com.google.common.base.Preconditions;

public final class AndPredicate<T> implements ObjectPredicate<T> {

  private final ObjectPredicate<T>[] clauses;

  public AndPredicate(ObjectPredicate<T>... clauses) {
    Preconditions.checkNotNull(clauses);
    Preconditions.checkArgument(clauses.length > 0);
    this.clauses = clauses;
  }

  @Override
  public boolean apply(T value) {
    for (ObjectPredicate<T> clause : clauses) {
      if (!clause.apply(value)) {
        return false;
      }
    }
    return true;
  }

}
