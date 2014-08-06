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

package com.cloudera.oryx.ml.mllib.als;

import org.apache.commons.math3.random.RandomGenerator;

import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.kafka.util.RandomDatumGenerator;

final class RandomALSDataGenerator implements RandomDatumGenerator<String,String> {

  private final int numUsers;
  private final int numProducts;
  private final int minRating;
  private final int maxRating;

  RandomALSDataGenerator(int numUsers,
                         int numProducts,
                         int minRating,
                         int maxRating) {
    this.numUsers = numUsers;
    this.numProducts = numProducts;
    this.minRating = minRating;
    this.maxRating = maxRating;
  }

  @Override
  public Pair<String,String> generate(int id, RandomGenerator random) {
    return new Pair<>(Integer.toString(id),
                      random.nextInt(numUsers) + "," +
                      random.nextInt(numProducts) + "," +
                      (random.nextInt(maxRating - minRating) + minRating));
  }

}
