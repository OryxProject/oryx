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

import java.io.Closeable;
import java.io.Serializable;

/**
 * Wraps access to a message queue {@code Producer}, including logic to instantiate the
 * object. This is a wrapper that can be serialized and re-create the {@code Producer}
 * remotely.
 */
public interface QueueProducer<K, M> extends Serializable, Closeable {

  void send(K key, M message);

  void send(M message);

  @Override
  void close();

}
