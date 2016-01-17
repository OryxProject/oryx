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

package com.cloudera.oryx.app.serving.als.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.collection.Pairs;

public final class TopNConsumerTest extends OryxTest {

  @Test
  public void testTopN() {
    int howMany = 5;
    TopNConsumer consumer = new TopNConsumer(howMany, value -> value[0], null, null);
    List<Integer> values = new ArrayList<>();
    int numValues = 100;
    for (int i = 0; i < numValues; i++) {
      values.add(i);
    }
    Collections.shuffle(values);
    for (int value : values) {
      consumer.accept(Integer.toString(value), new float[] { value });
    }
    List<Pair<String,Double>> topList = consumer.getTopN()
        .sorted(Pairs.orderBySecond(Pairs.SortOrder.DESCENDING))
        .collect(Collectors.toList());
    assertEquals(howMany, topList.size());
    int expected = numValues - 1;
    for (Pair<String,Double> p : topList) {
      assertEquals(Integer.toString(expected), p.getFirst());
      assertEquals(expected, (double) p.getSecond());
      expected--;
    }
  }

}
