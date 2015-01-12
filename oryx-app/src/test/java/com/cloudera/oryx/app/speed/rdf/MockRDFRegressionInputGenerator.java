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

package com.cloudera.oryx.app.speed.rdf;

import org.apache.commons.math3.random.RandomGenerator;

import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.kafka.util.DatumGenerator;

final class MockRDFRegressionInputGenerator implements DatumGenerator<String,String> {

  @Override
  public Pair<String, String> generate(int id, RandomGenerator random) {
    boolean positive = id % 2 != 0;
    // Dummy decision rule is > 3.14
    double predictor = positive ? 3.14 + id : 3.14 - id;
    // Constructed so that means are about (1+3+5+7+9)/5 = 5 and -(0+2+4+6+8)/5 = -4
    double target = positive ? (id % 10) : -(id % 10);
    return new Pair<>("", predictor + "," + target);
  }

}
