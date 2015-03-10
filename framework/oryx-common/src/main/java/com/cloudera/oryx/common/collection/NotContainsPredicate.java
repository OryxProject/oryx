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

import com.google.common.base.Preconditions;
import net.openhft.koloboke.function.Predicate;

import java.util.Collection;

/**
 * Predicate that is true when a value is not in a given collection.
 */
public final class NotContainsPredicate<T> implements Predicate<T> {

  private final Collection<T> values;

  public NotContainsPredicate(Collection<T> values) {
    Preconditions.checkNotNull(values);
    this.values = values;
  }

  @Override
  public boolean test(T value) {
    return !values.contains(value);
  }

}
