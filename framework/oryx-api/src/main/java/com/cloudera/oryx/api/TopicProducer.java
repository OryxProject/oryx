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

import java.io.Closeable;

/**
 * Wraps access to a message topic {@code Producer}, including logic to instantiate the
 * object. This is a wrapper that can be serialized and re-create the {@code Producer}
 * remotely.
 *
 * @param <K> key type to send
 * @param <M> message type to send
 */
public interface TopicProducer<K, M> extends Closeable {

  /**
   * @return broker(s) that the producer is sending to
   */
  String getUpdateBroker();

  /**
   * @return topic that the producer is sending to
   */
  String getTopic();

  /**
   * @param key key to send to the topic
   * @param message message to send with key to the topic
   */
  void send(K key, M message);

  /**
   * @param message message to send (with no key) to the topic
   */
  void send(M message);

  @Override
  void close();

}
