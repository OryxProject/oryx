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

import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;

import com.cloudera.oryx.app.kmeans.ClusterInfo;
import com.cloudera.oryx.app.kmeans.DistanceFn;

final class DaviesBouldinIndex extends AbstractKMeansEvaluation {

  DaviesBouldinIndex(List<ClusterInfo> clusters) {
    super(clusters);
  }

  /**
   * @param evalData data for evaluation
   * @return the Davies-Bouldin Index (http://en.wikipedia.org/wiki/Cluster_analysis#Internal_evaluation);
   *  lower is better
   */
  @Override
  double evaluate(JavaRDD<Vector> evalData) {
    Map<Integer,ClusterMetric> clusterMetricsByID = fetchClusterMetrics(evalData).collectAsMap();
    double totalDBIndex = 0.0;
    Map<Integer,ClusterInfo> clustersByID = getClustersByID();
    DistanceFn<double[]> distanceFn = getDistanceFn();
    for (Map.Entry<Integer,ClusterInfo> entryI : clustersByID.entrySet()) {
      double maxDBIndex = 0.0;
      Integer idI = entryI.getKey();
      double[] centerI = entryI.getValue().getCenter();
      double clusterScatter1 = clusterMetricsByID.get(idI).getMeanDist();
      // this inner loop should not be set to j = (i+1) as DB Index computation is not symmetric.
      // For a given cluster i, we look for a cluster j that maximizes
      // the ratio of (the sum of average distances from points in cluster i to its center and
      // points in cluster j to its center) to (the distance between cluster i and cluster j).
      // The key here is the Maximization of the DB Index for a cluster:
      // the cluster that maximizes this ratio may be j for i but not necessarily i for j
      for (Map.Entry<Integer,ClusterInfo> entryJ : clustersByID.entrySet()) {
        Integer idJ = entryJ.getKey();
        if (!idI.equals(idJ)) {
          double[] centerJ = entryJ.getValue().getCenter();
          double clusterScatter2 = clusterMetricsByID.get(idJ).getMeanDist();
          double dbIndex = (clusterScatter1 + clusterScatter2) / distanceFn.distance(centerI, centerJ);
          maxDBIndex = Math.max(maxDBIndex, dbIndex);
        }
      }
      totalDBIndex += maxDBIndex;
    }

    return totalDBIndex / clustersByID.size();
  }

}
