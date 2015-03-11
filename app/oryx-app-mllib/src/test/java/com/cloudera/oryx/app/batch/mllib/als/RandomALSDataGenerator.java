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

package com.cloudera.oryx.app.batch.mllib.als;

import org.apache.commons.math3.random.RandomGenerator;

import com.cloudera.oryx.app.als.ALSUtilsTest;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.kafka.util.DatumGenerator;

/**
 * Generates random "user,product,rating,timestamp" data. The user is an integer chosen from
 * [0,numUsers) uniformly at random, and likewise for the product, from [0,numProducts).
 * User and product IDs are actually non-numeric, and generated as "A0", "B1", ... , "A26", etc.
 * Rating is an integer chosen from [minRating,maxRating]. Timestamp is the current time.
 */
final class RandomALSDataGenerator implements DatumGenerator<String,String> {

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
    String userString = ALSUtilsTest.idToStringID(random.nextInt(numUsers));
    String itemString = ALSUtilsTest.idToStringID(random.nextInt(numProducts));
    int rating = random.nextInt(maxRating - minRating + 1) + minRating;
    String datum = userString + "," +  itemString + "," + rating + "," + System.currentTimeMillis();
    return new Pair<>(Integer.toString(id), datum);
  }

}
