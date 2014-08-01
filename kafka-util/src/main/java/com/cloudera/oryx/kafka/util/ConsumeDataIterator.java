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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.AbstractIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;

import com.cloudera.oryx.common.collection.CloseableIterator;

final class ConsumeDataIterator
    extends AbstractIterator<String[]> implements CloseableIterator<String[]> {

  private final ConsumerConnector consumer;
  private final Iterator<MessageAndMetadata<String,String>> iterator;
  private boolean closed;

  ConsumeDataIterator(String topic, ConsumerConnector consumer) {
    this.consumer = consumer;
    Map<String,Integer> topicCountMap = new HashMap<>();
    topicCountMap.put(topic, 1);

    Map<String,List<KafkaStream<String,String>>> consumerMap =
        consumer.createMessageStreams(topicCountMap,
                                      new StringDecoder(null),
                                      new StringDecoder(null));
    KafkaStream<String,String> stream = consumerMap.get(topic).get(0);
    this.iterator = stream.iterator();
  }

  @Override
  protected String[] computeNext() {
    if (iterator.hasNext()) {
      MessageAndMetadata<String,String> mm = iterator.next();
      return new String[] { mm.key(), mm.message() };
    } else {
      close();
      return endOfData();
    }
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      consumer.shutdown();
    }
  }
}
