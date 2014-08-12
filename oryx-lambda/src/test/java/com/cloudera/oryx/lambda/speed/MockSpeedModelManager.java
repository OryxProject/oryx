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

package com.cloudera.oryx.lambda.speed;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;

import com.cloudera.oryx.lambda.KeyMessage;

public final class MockSpeedModelManager implements SpeedModelManager<String,String,String> {

  private static final List<KeyMessage<String,String>> holder = new ArrayList<>();

  static List<KeyMessage<String,String>> getIntervalDataHolder() {
    return holder;
  }

  @Override
  public void consume(Iterator<KeyMessage<String,String>> updateIterator) {
    while (updateIterator.hasNext()) {
      KeyMessage<String,String> update = updateIterator.next();
      holder.add(new KeyMessage<>(update.getKey(), update.getMessage()));
    }
  }

  @Override
  public Collection<String> buildUpdates(JavaPairRDD<String,String> newData) {
    return newData.values().collect();
  }

  @Override
  public void close() {
    // do nothing
  }

}
