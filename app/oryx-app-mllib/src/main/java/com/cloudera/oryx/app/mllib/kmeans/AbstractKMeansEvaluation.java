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

import com.google.common.base.Preconditions;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple2;

import com.cloudera.oryx.app.kmeans.ClusterInfo;
import com.cloudera.oryx.app.kmeans.DistanceFn;
import com.cloudera.oryx.app.kmeans.SquaredDistanceFn;

abstract class AbstractKMeansEvaluation implements Serializable {

  private final DistanceFn<double[]> distanceFn;
  private final int numClusters;
  private final List<ClusterInfo> clusters;

  AbstractKMeansEvaluation(List<ClusterInfo> clusters) {
    this.distanceFn = new SquaredDistanceFn(); // for now using Squared Euclidean only
    this.clusters = clusters;
    this.numClusters = clusters.size();
  }

  final DistanceFn<double[]> getDistanceFn() {
    return distanceFn;
  }

  final int getNumClusters() {
    return numClusters;
  }

  final List<ClusterInfo> getClusters() {
    return clusters;
  }

  abstract double evaluate(JavaRDD<Vector> evalData);

  JavaPairRDD<Integer, Tuple2<Double, Long>> fetchClusterSumDistanceAndCounts(
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
