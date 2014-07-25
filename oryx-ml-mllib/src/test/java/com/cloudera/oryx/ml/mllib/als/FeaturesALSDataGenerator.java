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

import com.cloudera.oryx.kafka.util.RandomDatumGenerator;

/**
 * Creates random data where products associated to users fall neatly into
 * a given number of distinct categories, which is most naturally modeled
 * by a factorization with that same number of features.
 */
final class FeaturesALSDataGenerator implements RandomDatumGenerator<String> {

  private final int numUsers;
  private final int numProducts;
  private final int features;

  FeaturesALSDataGenerator(int numUsers, int numProducts, int features) {
    this.numUsers = numUsers;
    this.numProducts = numProducts;
    this.features = features;
  }

  @Override
  public String generate(int id, RandomGenerator random) {
    int user = random.nextInt(numUsers);
    int randProduct = random.nextInt(numProducts);
    // Want product === user mod features
    int product = ((user % features) + (randProduct / features) * features) % numProducts;
    return user + "," + product + ",1.0";
  }

}
