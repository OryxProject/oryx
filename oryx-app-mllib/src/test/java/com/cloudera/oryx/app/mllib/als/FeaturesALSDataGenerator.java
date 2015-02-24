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

package com.cloudera.oryx.app.mllib.als;

import org.apache.commons.math3.random.RandomGenerator;

import com.cloudera.oryx.app.als.ALSUtilsTest;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.kafka.util.DatumGenerator;

/**
 * Creates random data where products associated to users fall neatly into
 * a given number of distinct categories, which is most naturally modeled
 * by a factorization with that same number of features.
 * User and product IDs are actually non-numeric, and generated as "A0", "B1", ... , "A26", etc.
 */
final class FeaturesALSDataGenerator implements DatumGenerator<String,String> {

  private final int numUsers;
  private final int numProducts;
  private final int features;

  FeaturesALSDataGenerator(int numUsers, int numProducts, int features) {
    this.numUsers = numUsers;
    this.numProducts = numProducts;
    this.features = features;
  }

  @Override
  public Pair<String,String> generate(int id, RandomGenerator random) {
    int user = random.nextInt(numUsers);
    int randProduct = random.nextInt(numProducts);
    // Want product === user mod features
    int product = ((user % features) + (randProduct / features) * features) % numProducts;
    String datum = ALSUtilsTest.idToStringID(user) + "," + ALSUtilsTest.idToStringID(product) +
        ",1," + System.currentTimeMillis();
    return new Pair<>(Integer.toString(id), datum);
  }

}
