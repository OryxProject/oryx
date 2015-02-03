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

package com.cloudera.oryx.app.speed.kmeans;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.random.RandomGenerator;

import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.text.TextUtils;
import com.cloudera.oryx.kafka.util.DatumGenerator;

public final class MockKMeansInputGenerator implements DatumGenerator<String,String> {

  // Corresponds to points near the three cluster centers in the dummy K-means model
  static final double[][] UPDATE_POINTS = {
      { 1.0, 1.0 }, { 2.0, -2.0 }, { -2.0, 0.0 }
  };

  @Override
  public Pair<String, String> generate(int id, RandomGenerator random) {
    return new Pair<>(Integer.toString(id),
                      TextUtils.joinJSON(arrayToList(UPDATE_POINTS[id % UPDATE_POINTS.length])));
  }

  private static List<Double> arrayToList(double[] arr) {
    List<Double> list = new ArrayList<>(arr.length);
    for (double d : arr) {
      list.add(d);
    }
    return list;
  }
}
