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

package com.cloudera.oryx.app.speed.als;

import org.apache.commons.math3.random.RandomGenerator;

import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.kafka.util.DatumGenerator;

public final class MockInputGenerator implements DatumGenerator<String,String> {

  @Override
  public Pair<String,String> generate(int id, RandomGenerator random) {
    if (id < 5) {
      return new Pair<>("", (100 + id) + "," + (1 + id) + ",1," + System.currentTimeMillis());
    } else {
      return new Pair<>("", (1 + id) + "," + (100 + id) + ",1," + System.currentTimeMillis());
    }
  }

}
