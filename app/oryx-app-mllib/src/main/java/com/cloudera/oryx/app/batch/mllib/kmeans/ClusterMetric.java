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

package com.cloudera.oryx.app.batch.mllib.kmeans;

import java.io.Serializable;

final class ClusterMetric implements Serializable {

  private final long count;
  private final double sumDist;
  private final double sumSquaredDist;

  ClusterMetric(long count, double sumDist, double sumSquaredDist) {
    this.count = count;
    this.sumDist = sumDist;
    this.sumSquaredDist = sumSquaredDist;
  }

  long getCount() {
    return count;
  }

  double getSumDist() {
    return sumDist;
  }

  double getSumSquaredDist() {
    return sumSquaredDist;
  }

  double getMeanDist() {
    return sumDist / count;
  }

  ClusterMetric add(ClusterMetric other) {
    return new ClusterMetric(count + other.getCount(),
                             sumDist + other.getSumDist(),
                             sumSquaredDist + other.getSumSquaredDist());
  }

}
