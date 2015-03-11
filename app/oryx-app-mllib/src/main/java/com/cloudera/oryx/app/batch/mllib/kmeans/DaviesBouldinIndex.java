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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import com.cloudera.oryx.app.kmeans.ClusterInfo;
import com.cloudera.oryx.app.kmeans.DistanceFn;

final class DaviesBouldinIndex extends AbstractKMeansEvaluation {

  private static final Logger log = LoggerFactory.getLogger(DaviesBouldinIndex.class);

  DaviesBouldinIndex(List<ClusterInfo> clusters) {
    super(clusters);
  }

  @Override
  double evaluate(JavaRDD<Vector> evalData) {
    return daviesBouldinIndex(fetchClusterSumDistanceAndCounts(evalData).collectAsMap());
  }

  /**
   * Computes the Davies-Bouldin Index
   * (http://en.wikipedia.org/wiki/Cluster_analysis#Internal_evaluation)
   * @param clusterSumDistAndCounts data for evaluation
   * @return Davies Bouldin index measure of clustering quality; lower is better
   */
  double daviesBouldinIndex(Map<Integer,Tuple2<Double,Long>> clusterSumDistAndCounts) {
    double totalDBIndex = 0.0;
    List<ClusterInfo> clusters = getClusters();
    int numClusters = getNumClusters();
    DistanceFn<double[]> distanceFn = getDistanceFn();
    for (int i = 0; i < numClusters; i++) {
      double maxDBIndex = 0.0;
      double[] center = clusters.get(i).getCenter();
      double clusterScatter1 = scatter(clusterSumDistAndCounts.get(i));
      // this inner loop should not be set to j = (i+1) as DB Index computation is not symmetric.
      // For a given cluster i, we look for a cluster j that maximizes
      // the ratio of (the sum of average distances from points in cluster i to its center and
      // points in cluster j to its center) to (the distance between cluster i and cluster j).
      // The key here is the Maximization of the DB Index for a cluster:
      // the cluster that maximizes this ratio may be j for i but not necessarily i for j
      for (int j = 0; j < numClusters; j++) {
        if (i != j) {
          double dbIndex = (clusterScatter1 + scatter(clusterSumDistAndCounts.get(j))) /
              distanceFn.distance(center, clusters.get(j).getCenter());
          if (dbIndex > maxDBIndex) {
            maxDBIndex = dbIndex;
          }
        }
      }
      totalDBIndex += maxDBIndex;
    }

    double daviesBouldinIndex = totalDBIndex / numClusters;
    log.info("Computed Davies-Bouldin Index for {} clusters: {}", numClusters, daviesBouldinIndex);
    return daviesBouldinIndex;
  }

  private static double scatter(Tuple2<Double,Long> sumDistAndCount) {
    return sumDistAndCount._1() / sumDistAndCount._2();
  }

}
