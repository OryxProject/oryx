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

package com.cloudera.oryx.lambda.serving;

import java.util.ArrayList;
import java.util.List;

import com.cloudera.oryx.api.TopicProducer;
import com.cloudera.oryx.common.collection.Pair;

public final class MockTopicProducer implements TopicProducer<String,String> {

  private static final List<Pair<String,String>> DATA = new ArrayList<>();

  public static List<Pair<String,String>> getData() {
    return DATA;
  }

  @Override
  public void send(String key, String message) {
    DATA.add(new Pair<>(key, message));
  }

  @Override
  public void send(String message) {
    send(null, message);
  }

  @Override
  public void close() {
    // do nothing
  }

}
