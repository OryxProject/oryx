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

package com.cloudera.oryx.app.mllib.rdf;

import java.util.ArrayList;
import java.util.List;

import com.clearspring.analytics.util.Preconditions;
import org.apache.commons.math3.random.RandomGenerator;

import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.text.TextUtils;
import com.cloudera.oryx.kafka.util.DatumGenerator;

/**
 * Generations n+2 dimensional data, where the first column is an ID, the next n
 * columns are categorical values in [A,B] and the last is equal to the number of features
 * that are A. The resulting data is returned as a CSV string.
 */
final class RandomNumericRDFDataGenerator implements DatumGenerator<String,String> {

  private final int n;

  RandomNumericRDFDataGenerator(int n) {
    Preconditions.checkArgument(n >= 1);
    this.n = n;
  }

  @Override
  public Pair<String,String> generate(int id, RandomGenerator random) {
    List<String> elements = new ArrayList<>(n + 2);
    elements.add(Integer.toString(id));
    int count = 0;
    for (int i = 0; i < n; i++) {
      boolean positive = random.nextBoolean();
      elements.add(positive ? "A" : "B");
      if (positive) {
        count++;
      }
    }
    elements.add(Integer.toString(count));
    return new Pair<>(Integer.toString(id), TextUtils.joinCSV(elements));
  }

}
