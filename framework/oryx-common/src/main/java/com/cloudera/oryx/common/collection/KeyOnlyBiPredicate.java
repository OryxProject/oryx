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

import net.openhft.koloboke.function.BiPredicate;
import net.openhft.koloboke.function.Predicate;

/**
 * {@link BiPredicate} that only applies a {@link Predicate} to the keys and ignores values.
 *
 * @param <K> key type
 * @param <V> value type (unused)
 */
public final class KeyOnlyBiPredicate<K,V> implements BiPredicate<K,V> {

  private final Predicate<K> keyPredicate;

  public KeyOnlyBiPredicate(Predicate<K> keyPredicate) {
    this.keyPredicate = keyPredicate;
  }

  @Override
  public boolean test(K key, V value) {
    return keyPredicate.test(key);
  }

}
