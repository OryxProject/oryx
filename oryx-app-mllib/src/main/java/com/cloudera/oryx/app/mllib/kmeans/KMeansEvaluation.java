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

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import com.cloudera.oryx.app.kmeans.ClusterInfo;
import com.cloudera.oryx.app.kmeans.DistanceFn;
import com.cloudera.oryx.app.kmeans.SquaredDistanceFn;

final class KMeansEvaluation implements Serializable {

  private static final Logger log = LoggerFactory.getLogger(KMeansEvaluation.class);

  private final DistanceFn<double[]> distanceFn;
  private final List<ClusterInfo> clusters;
  private final int numClusters;

  KMeansEvaluation(List<ClusterInfo> clusters) {
    this.distanceFn = new SquaredDistanceFn(); //for now using Squared Euclidean only
    this.clusters = clusters;
    this.numClusters = clusters.size();
  }

  /**
   * Computes the Davies-Bouldin Index
   * (http://en.wikipedia.org/wiki/Cluster_analysis#Internal_evaluation)
   * @param evalData data for evaluation
   * @return Davies Bouldin index measure of clustering quality; lower is better
   */
  double daviesBouldinIndex(JavaRDD<Vector> evalData) {
    Map<Integer, Tuple2<Double, Long>> clusterSumDistAndCounts =
        fetchClusterSumDistanceAndCounts(evalData).collectAsMap();

    double totalDBIndex = 0.0;
    for (int i = 0; i < numClusters; i++) {
      double maxDBIndex = 0.0;

      ClusterInfo c1 = clusters.get(i);
      double clusterScatter1 =
          clusterSumDistAndCounts.get(i)._1() / clusterSumDistAndCounts.get(i)._2();
      for (int j = 0; j < numClusters; j++) {
        if (i != j) {
          ClusterInfo c2 = clusters.get(j);

          double clusterScatter2 =
              clusterSumDistAndCounts.get(j)._1() / clusterSumDistAndCounts.get(j)._2();
          double dbIndex = (clusterScatter1 + clusterScatter2) /
              distanceFn.distance(c1.getCenter(), c2.getCenter());

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

  /**
   * Computes the Dunn Index of a given clustering
   * (http://en.wikipedia.org/wiki/Dunn_index)
   * @param evalData data for evaluation
   * @return Dunn Index; higher is better
   */
  double dunnIndex(JavaRDD<Vector> evalData) {
    List<Tuple2<Integer, Tuple2<Double, Long>>> clusterSumDistAndCounts =
        fetchClusterSumDistanceAndCounts(evalData).collect();

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
      for (int j = 0; j < numClusters; j++) {
        if (i != j) {
          double distance = distanceFn.distance(center, clusters.get(j).getCenter());
          if (minInterClusterDistance > distance) {
            minInterClusterDistance = distance;
          }
        }
      }
    }

    double dunnIndex = minInterClusterDistance / maxIntraClusterDistance;
    log.info("Computed Dunn Index for {} clusters: {}", numClusters, dunnIndex);

    return dunnIndex;
  }

  private JavaPairRDD<Integer, Tuple2<Double, Long>> fetchClusterSumDistanceAndCounts(
      JavaRDD<Vector> evalData) {

    return evalData.mapToPair(new PairFunction<Vector, Integer, Tuple2<Double, Long>>() {
      @Override
      public Tuple2<Integer, Tuple2<Double, Long>> call(Vector vector) {
        double closestDist = Double.POSITIVE_INFINITY;
        int minCluster = -1;
        double[] vec = vector.toArray();

        for (int i = 0; i < numClusters; i++) {
          ClusterInfo cluster = clusters.get(i);
          double distance = distanceFn.distance(cluster.getCenter(), vec);
          if (distance < closestDist) {
            closestDist = distance;
            minCluster = i;
          }
        }
        Preconditions.checkState(minCluster >= 0);
        Preconditions.checkState(!Double.isInfinite(closestDist) && !Double.isNaN(closestDist));
        return new Tuple2<>(minCluster, new Tuple2<>(closestDist, 1L));
      }
    }).reduceByKey(
        new Function2<Tuple2<Double, Long>, Tuple2<Double, Long>, Tuple2<Double, Long>>() {
          @Override
          public Tuple2<Double, Long> call(Tuple2<Double, Long> v1, Tuple2<Double, Long> v2) {
            double intraClusterDistanceSum = v1._1() + v2._1();
            long clusteredPointsCount = v1._2() + v2._2();

            return new Tuple2<>(intraClusterDistanceSum, clusteredPointsCount);
          }
        });
  }

}
