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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

import com.cloudera.oryx.app.kmeans.ClusterInfo;
import com.cloudera.oryx.app.kmeans.DistanceFn;
import com.cloudera.oryx.app.kmeans.KMeansPMMLUtils;
import com.cloudera.oryx.app.kmeans.KMeansPMMLUtilsTest;
import com.cloudera.oryx.app.kmeans.SquaredDistanceFn;
import com.cloudera.oryx.common.OryxTest;

public abstract class AbstractKMeansEvalTest extends OryxTest {

  protected static final int NUM_CLUSTERS = 3;
  protected static final List<double[]> POINTS =
      Arrays.asList(new double[][]{
          {1.0, 0.0}, {2.0, -2.0}, {2.0, 0.0},
          {-2.0, 0.0}, {-0.5, -1.0}, {-0.5, 1.0}
      });
  protected static final DistanceFn<double[]> DISTANCE_FN = new SquaredDistanceFn();

  protected static List<ClusterInfo> getClusters() {
    return KMeansPMMLUtils.read(KMeansPMMLUtilsTest.buildDummyClusteringModel());
  }

  protected static Map<Integer, Tuple2<Double, Long>> fetchClusterSumDistanceAndCounts(
      List<ClusterInfo> clusters,
      List<double[]> points) {

    Map<Integer, Tuple2<Double, Long>> clusterSumDistAndCountsMap = new HashMap<>();

    for (double[] point : points) {
      Tuple2<Integer, Double> clusterAssign = pointClusterAssign(clusters, point);
      int clusterId = clusterAssign._1();
      double closestDist = clusterAssign._2();

      if (clusterSumDistAndCountsMap.isEmpty() ||
          !clusterSumDistAndCountsMap.containsKey(clusterId)) {
        clusterSumDistAndCountsMap.put(clusterId, new Tuple2<>(closestDist, 1L));
      } else {
        Tuple2<Double, Long> tuple = clusterSumDistAndCountsMap.get(clusterId);
        double avgSumDist = tuple._1() + closestDist;
        long count = tuple._2() + 1L;
        clusterSumDistAndCountsMap.put(clusterId, new Tuple2<>(avgSumDist, count));
      }
    }

    return clusterSumDistAndCountsMap;
  }

  protected static Tuple2<Integer, Double> pointClusterAssign(List<ClusterInfo> clusters,
                                                            double[] point) {
    double closestDist = Double.POSITIVE_INFINITY;
    int minCluster = -1;

    for (int i = 0; i < clusters.size(); i++) {
      ClusterInfo cluster = clusters.get(i);
      double distance = DISTANCE_FN.distance(cluster.getCenter(), point);
      if (distance < closestDist) {
        closestDist = distance;
        minCluster = i;
      }
    }

    return new Tuple2<>(minCluster, closestDist);
  }
}
