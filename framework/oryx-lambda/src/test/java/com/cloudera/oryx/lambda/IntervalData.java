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

import java.util.Collection;

import scala.Tuple2;

/**
 * A helper class for testing, which encapsulates all tuples received in an interval,
 * the time it was received, and the state of all past data received before the interval
 * at that time.
 */
final class IntervalData<K,V> {

  private final long timestamp;
  private final Collection<Tuple2<K,V>> currentData;
  private final Collection<Tuple2<K,V>> pastData;

  IntervalData(long timestamp,
               Collection<Tuple2<K,V>> currentData,
               Collection<Tuple2<K,V>> pastData) {
    this.timestamp = timestamp;
    this.currentData = currentData;
    this.pastData = pastData;
  }

  long getTimestamp() {
    return timestamp;
  }

  Collection<Tuple2<K,V>> getCurrentData() {
    return currentData;
  }

  Collection<Tuple2<K,V>> getPastData() {
    return pastData;
  }

  @Override
  public String toString() {
    return timestamp + " : " + currentData.size() + " current, " + pastData.size()  + " past";
  }

}
