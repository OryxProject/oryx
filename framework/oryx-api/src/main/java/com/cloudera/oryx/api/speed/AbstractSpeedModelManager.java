/*
 * Copyright (c) 2016, Cloudera, Inc. All Rights Reserved.
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

package com.cloudera.oryx.api.speed;

import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.api.KeyMessage;

/**
 * Convenience implementation of {@link SpeedModelManager} that provides default implementations.
 *
 * @param <K> type of key read from input topic
 * @param <M> type of message read from input topic
 * @param <U> type of update message read/written
 * @since 2.3.0
 */
public abstract class AbstractSpeedModelManager<K,M,U> implements SpeedModelManager<K,M,U> {

  private static final Logger log = LoggerFactory.getLogger(AbstractSpeedModelManager.class);

  @Override
  public void consume(Iterator<KeyMessage<String,U>> updateIterator, Configuration hadoopConf) throws IOException {
    while (updateIterator.hasNext()) {
      try {
        KeyMessage<String,U> km = updateIterator.next();
        String key = km.getKey();
        U message = km.getMessage();
        Objects.requireNonNull(key);
        consumeKeyMessage(key, message, hadoopConf);
      } catch (Exception e) {
        log.warn("Exception while processing message; continuing", e);
      }
    }
  }

  /**
   * Convenience method that is called by the default implementation of
   * {@link #consume(Iterator, Configuration)}, to process one key-message pair.
   * It does nothing, except log the message. This should generally be overridden
   * if and only if {@link #consume(Iterator, Configuration)} is not.
   *
   * @param key key to process (non-null)
   * @param message message to process
   * @param hadoopConf Hadoop configuration for process
   * @throws IOException if an error occurs while processing the message
   * @since 2.3.0
   */
  public void consumeKeyMessage(String key, U message, Configuration hadoopConf) throws IOException {
    log.info("{} : {}", key, message);
  }

}
