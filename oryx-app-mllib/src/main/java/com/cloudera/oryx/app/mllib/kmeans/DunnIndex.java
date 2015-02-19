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

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import com.cloudera.oryx.app.kmeans.ClusterInfo;

final class DunnIndex extends AbstractKMeansEvaluation {

  private static final Logger log = LoggerFactory.getLogger(DunnIndex.class);

  DunnIndex(List<ClusterInfo> clusters) {
    super(clusters);
  }

  @Override
  double evaluate(JavaRDD<Vector> evalData) {
    return dunnIndex(fetchClusterSumDistanceAndCounts(evalData).collect());
  }

  /**
   * Computes the Dunn Index of a given clustering
   * (http://en.wikipedia.org/wiki/Dunn_index)
   * @param clusterSumDistAndCounts data for evaluation
   * @return Dunn Index; higher is better
   */
  double dunnIndex(List<Tuple2<Integer, Tuple2<Double, Long>>> clusterSumDistAndCounts) {
    double maxIntraClusterDistance = 0.0;
    for (Tuple2<Integer, Tuple2<Double, Long>> entry : clusterSumDistAndCounts) {
      double intraClusterDistance = entry._2()._1() / entry._2()._2();
      if (maxIntraClusterDistance < intraClusterDistance) {
        maxIntraClusterDistance = intraClusterDistance;
      }
    }

    double minInterClusterDistance = Double.POSITIVE_INFINITY;

    for (int i = 0; i < numClusters; i++) {
      double[] center = clusters.get(i).getCenter();
      // Distances are symmetric, hence d(i,j) == d(j,i)
      for (int j = i + 1; j < numClusters; j++) {
        double distance = distanceFn.distance(center, clusters.get(j).getCenter());
        if (distance < minInterClusterDistance) {
          minInterClusterDistance = distance;
        }
      }
    }

    double dunnIndex = minInterClusterDistance / maxIntraClusterDistance;
    log.info("Computed Dunn Index for {} clusters: {}", numClusters, dunnIndex);
    return dunnIndex;
  }

}
