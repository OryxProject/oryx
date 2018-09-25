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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;

import com.cloudera.oryx.app.kmeans.ClusterInfo;
import com.cloudera.oryx.app.kmeans.DistanceFn;

final class DunnIndex extends AbstractKMeansEvaluation {

  DunnIndex(Collection<ClusterInfo> clusters) {
    super(clusters);
  }

  /**
   * @param evalData data for evaluation
   * @return the Dunn Index of a given clustering
   *  (https://en.wikipedia.org/wiki/Cluster_analysis#Internal_evaluation); higher is better
   */
  @Override
  double evaluate(JavaRDD<Vector> evalData) {
    // Intra-cluster distance is mean distance to centroid
    double maxIntraClusterDistance =
        fetchClusterMetrics(evalData).values().mapToDouble(ClusterMetric::getMeanDist).max();

    // Inter-cluster distance is distance between centroids
    double minInterClusterDistance = Double.POSITIVE_INFINITY;
    List<ClusterInfo> clusters = new ArrayList<>(getClustersByID().values());
    DistanceFn<double[]> distanceFn = getDistanceFn();
    for (int i = 0; i < clusters.size(); i++) {
      double[] centerI = clusters.get(i).getCenter();
      // Distances are symmetric, hence d(i,j) == d(j,i)
      for (int j = i + 1; j < clusters.size(); j++) {
        double[] centerJ = clusters.get(j).getCenter();
        minInterClusterDistance = Math.min(minInterClusterDistance, distanceFn.applyAsDouble(centerI, centerJ));
      }
    }

    return minInterClusterDistance / maxIntraClusterDistance;
  }

}
