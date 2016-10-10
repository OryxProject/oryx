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

package com.cloudera.oryx.kafka.util;

import java.util.Iterator;
import java.util.Objects;

import com.google.common.collect.AbstractIterator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.cloudera.oryx.api.KeyMessage;
import com.cloudera.oryx.api.KeyMessageImpl;
import com.cloudera.oryx.common.collection.CloseableIterator;

/**
 * An iterator over records in a Kafka topic.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class ConsumeDataIterator<K,V>
    extends AbstractIterator<KeyMessage<K,V>>
    implements CloseableIterator<KeyMessage<K,V>> {

  private static final long MIN_POLL_MS = 1;
  private static final long MAX_POLL_MS = 1000;

  private final KafkaConsumer<K,V> consumer;
  private volatile Iterator<ConsumerRecord<K,V>> iterator;
  private volatile boolean closed;

  public ConsumeDataIterator(KafkaConsumer<K,V> consumer) {
    this.consumer = Objects.requireNonNull(consumer);
  }

  @Override
  protected KeyMessage<K,V> computeNext() {
    if (iterator == null || !iterator.hasNext()) {
      try {
        long timeout = MIN_POLL_MS;
        ConsumerRecords<K, V> records;
        while ((records = consumer.poll(timeout)).isEmpty()) {
          timeout = Math.min(MAX_POLL_MS, timeout * 2);
        }
        iterator = records.iterator();
      } catch (Exception e) {
        consumer.close();
        return endOfData();
      }
    }
    ConsumerRecord<K,V> mm = iterator.next();
    return new KeyMessageImpl<>(mm.key(), mm.value());
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      consumer.wakeup();
    }
  }
}
