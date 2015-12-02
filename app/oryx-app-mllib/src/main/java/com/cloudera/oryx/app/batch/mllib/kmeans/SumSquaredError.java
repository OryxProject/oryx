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

package com.cloudera.oryx.app.batch.mllib.kmeans;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;

import com.cloudera.oryx.app.kmeans.ClusterInfo;

final class SumSquaredError extends AbstractKMeansEvaluation {

  SumSquaredError(List<ClusterInfo> clusters) {
    super(clusters);
  }

  @Override
  double evaluate(JavaRDD<Vector> evalData) {
    return fetchClusterMetrics(evalData).values().mapToDouble(ClusterMetric::getSumSquaredDist).sum();
  }

}
