/*
 * Copyright (c) 2015, Cloudera and Intel, Inc. All Rights Reserved.
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

package com.cloudera.oryx.app.mllib.kmeans;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.random.RandomGenerator;

import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.text.TextUtils;
import com.cloudera.oryx.kafka.util.DatumGenerator;

final class RandomKMeansDataGenerator implements DatumGenerator<String,String> {

  private final int numberOfDimensions;

  RandomKMeansDataGenerator(int numberOfDimensions) {
    this.numberOfDimensions = numberOfDimensions;
  }

  @Override
  public Pair<String,String> generate(int id, RandomGenerator random) {
    List<String> elements = new ArrayList<>(numberOfDimensions);
    for (int i = 0; i < numberOfDimensions; i++) {
      double d = random.nextDouble();
      elements.add(Double.toString(d));
    }
    return new Pair<>(Integer.toString(id), TextUtils.joinCSV(elements));
  }
}
