/*
 * Copyright (c) 2014, Cloudera and Intel, Inc. All Rights Reserved.
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

package com.cloudera.oryx.ml.mllib.als;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.random.RandomGenerator;

import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.kafka.util.RandomDatumGenerator;

/**
 * Generates a synthetic data set over users 0-3, items 0-3. Users u interact with all items i
 * where i >= u. But then interactions where i == u are deleted. But then the interaction 0 -> 0
 * is restored.
 *
 * @see ALSModelContentIT
 */
final class ModelContentDataGenerator implements RandomDatumGenerator<String,String> {

  private static final int NUM_USERS_ITEMS = 4;

  private final List<String> data;

  ModelContentDataGenerator() {
    long startTime = System.currentTimeMillis();
    data = new ArrayList<>();
    for (int user = 0; user < NUM_USERS_ITEMS; user++) {
      for (int item = user; item < NUM_USERS_ITEMS; item++) {
        data.add(user + "," + item + ",1," + startTime++);
      }
    }
    for (int userItem = 0; userItem < NUM_USERS_ITEMS; userItem++) {
      data.add(userItem + "," + userItem + ",," + startTime++);
    }
    data.add("0,0,1," + startTime);
  }

  List<String> getSentData() {
    return data;
  }

  @Override
  public Pair<String,String> generate(int id, RandomGenerator random) {
    return new Pair<>(Integer.toString(id), data.get(id));
  }

}
