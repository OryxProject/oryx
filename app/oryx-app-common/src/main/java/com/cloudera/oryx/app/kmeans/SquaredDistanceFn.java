/*
 * Copyright (c) 2015, Cloudera, Inc. and Intel Corp. All Rights Reserved.
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

package com.cloudera.oryx.app.kmeans;

import com.google.common.base.Preconditions;

/**
 * Returns the squared Euclidean distance between two {@code double[]}.
 */
public final class SquaredDistanceFn implements DistanceFn<double[]> {

  @Override
  public double distance(double[] t1, double[] t2) {
    int length = t1.length;
    Preconditions.checkArgument(length == t2.length);
    double sumSq = 0.0;
    for (int i = 0; i < length; i++) {
      double diff = t1[i] - t2[i];
      sumSq += diff * diff;
    }
    return sumSq;
  }

}
