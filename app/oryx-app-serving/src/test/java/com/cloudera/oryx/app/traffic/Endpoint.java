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

import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;

import com.google.common.base.Preconditions;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;

/**
 * Represents and endpoint that should receive requests in a traffic test.
 */
public abstract class Endpoint {

  private final String path;
  private final double relativeProb;
  private final Mean meanTimeNanos;
  private final StandardDeviation stdevTimeNanos;

  protected Endpoint(String path, double relativeProb) {
    Preconditions.checkArgument(relativeProb > 0.0);
    this.path = path;
    this.relativeProb = relativeProb;
    meanTimeNanos = new Mean();
    stdevTimeNanos = new StandardDeviation();
  }

  double getRelativeProb() {
    return relativeProb;
  }

  synchronized void recordTiming(long timeNanos) {
    meanTimeNanos.increment(timeNanos);
    stdevTimeNanos.increment(timeNanos);
  }

  protected abstract Invocation makeInvocation(WebTarget target, String[] otherArgs, RandomGenerator random);

  @Override
  public synchronized String toString() {
    return path + "\tcount:" + meanTimeNanos.getN() + "\tmean: " +
        Math.round(meanTimeNanos.getResult() / 1_000_000.0) + "ms\tstdev: " +
        Math.round(stdevTimeNanos.getResult() / 1_000_000.0) + "ms";
  }

}
