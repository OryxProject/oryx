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

package com.cloudera.oryx.app.traffic;

import com.google.common.base.Preconditions;
import org.apache.commons.math3.random.RandomGenerator;

/**
 * Represents a collection {@link Endpoint}s that can be randomly chosen from
 * to generate a diverse set of traffic for testing.
 */
final class Endpoints {

  private final Endpoint[] endpoints;
  private final double[] cumulativeProbs;

  Endpoints(Endpoint... endpoints) {
    Preconditions.checkArgument(endpoints.length >= 1);
    this.endpoints = endpoints;

    double sum = 0.0;
    for (Endpoint endpoint : endpoints) {
      sum += endpoint.getRelativeProb();
    }
    cumulativeProbs = new double[endpoints.length];
    cumulativeProbs[0] = endpoints[0].getRelativeProb() / sum;
    for (int i = 1; i < cumulativeProbs.length; i++) {
      cumulativeProbs[i] = cumulativeProbs[i - 1] + endpoints[i].getRelativeProb() / sum;
    }
  }

  Endpoint[] getEndpoints() {
    return endpoints;
  }

  Endpoint chooseEndpoint(RandomGenerator random) {
    double p = random.nextDouble();
    int i = 0;
    while (i < cumulativeProbs.length && p >= cumulativeProbs[i]) {
      i++;
    }
    return endpoints[i];
  }

}
