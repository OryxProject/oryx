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

package com.cloudera.oryx.api;

import java.util.Objects;

/**
 * Simple value class encapsulating a key and message in a topic.
 *
 * @param <K> key type
 * @param <M> message type
 * @since 2.0.0
 */
public final class KeyMessageImpl<K,M> implements KeyMessage<K,M> {

  private final K key;
  private final M message;

  /**
   * @param key key
   * @param message message
   * @since 2.0.0
   */
  public KeyMessageImpl(K key, M message) {
    this.key = key;
    this.message = message;
  }

  @Override
  public K getKey() {
    return key;
  }

  @Override
  public M getMessage() {
    return message;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(key) ^ Objects.hashCode(message);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof KeyMessageImpl)) {
      return false;
    }
    @SuppressWarnings("unchecked")
    KeyMessageImpl<K,M> other = (KeyMessageImpl<K,M>) o;
    return Objects.equals(key, other.key) && Objects.equals(message, other.message);
  }

  @Override
  public String toString() {
    return key + "," + message;
  }

}
