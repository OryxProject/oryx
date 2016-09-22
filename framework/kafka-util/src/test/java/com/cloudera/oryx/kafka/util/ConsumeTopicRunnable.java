/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.cloudera.oryx.api.KeyMessage;

public final class ConsumeTopicRunnable implements Callable<Void> {

  private final Iterator<KeyMessage<String,String>> data;
  private final List<KeyMessage<String,String>> keyMessages;
  private final CountDownLatch runLatch;
  private final CountDownLatch messagesLatch;

  public ConsumeTopicRunnable(Iterator<KeyMessage<String,String>> data) {
    this(data, 0);
  }

  public ConsumeTopicRunnable(Iterator<KeyMessage<String,String>> data, int expectedMessages) {
    this.data = data;
    this.keyMessages = new ArrayList<>();
    this.runLatch = new CountDownLatch(1);
    this.messagesLatch = new CountDownLatch(expectedMessages);
  }

  @Override
  public Void call() {
    runLatch.countDown();
    data.forEachRemaining(datum -> {
      keyMessages.add(datum);
      messagesLatch.countDown();
    });
    return null;
  }

  public void awaitRun() throws InterruptedException {
    runLatch.await();
  }

  public void awaitMessages() throws InterruptedException {
    if (!messagesLatch.await(1, TimeUnit.MINUTES)) {
      throw new IllegalStateException(
          "Timed out waiting for " + messagesLatch.getCount() + " more messages " +
          "(got " + keyMessages.size() + " messages)");
    }
  }

  public List<KeyMessage<String,String>> getKeyMessages() {
    return keyMessages;
  }

  List<String> getKeys() {
    return keyMessages.stream().map(KeyMessage::getKey).collect(Collectors.toList());
  }

}
