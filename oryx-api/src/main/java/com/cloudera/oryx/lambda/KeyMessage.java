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

package com.cloudera.oryx.lambda;

import java.io.Serializable;

/**
 * Simple value class encapsulating a key and message in a queue.
 */
public final class KeyMessage<K,M> implements Serializable {

  private final K key;
  private final M message;

  public KeyMessage(K key, M message) {
    this.key = key;
    this.message = message;
  }

  public K getKey() {
    return key;
  }

  public M getMessage() {
    return message;
  }

  @Override
  public String toString() {
    return key + "," + message;
  }

}
