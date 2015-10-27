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

import java.io.Serializable;

/**
 * Simple interface encapsulating a key and message in a topic.
 *
 * @param <K> key type
 * @param <M> message type
 * @see KeyMessageImpl
 * @since 2.0.0
 */
public interface KeyMessage<K,M> extends Serializable {

  /**
   * @return key
   * @since 2.0.0
   */
  K getKey();

  /**
   * @return message
   * @since 2.0.0
   */
  M getMessage();

}
