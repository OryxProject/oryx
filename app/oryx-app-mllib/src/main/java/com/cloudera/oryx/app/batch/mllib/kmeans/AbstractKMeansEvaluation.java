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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple2;

import com.cloudera.oryx.app.kmeans.ClusterInfo;
import com.cloudera.oryx.app.kmeans.DistanceFn;
import com.cloudera.oryx.app.kmeans.EuclideanDistanceFn;

abstract class AbstractKMeansEvaluation implements Serializable {

  private final DistanceFn<double[]> distanceFn;
  private final Map<Integer,ClusterInfo> clusters;

  AbstractKMeansEvaluation(List<ClusterInfo> clusterList) {
    this.distanceFn = new EuclideanDistanceFn(); // for now using Euclidean only
    this.clusters = new HashMap<>();
    for (ClusterInfo info : clusterList) {
      clusters.put(info.getID(), info);
    }
  }

  final DistanceFn<double[]> getDistanceFn() {
    return distanceFn;
  }

  final Map<Integer,ClusterInfo> getClustersByID() {
    return clusters;
  }

  abstract double evaluate(JavaRDD<Vector> evalData);

  /**
   * @param evalData points to cluster for evaluation
   * @return cluster IDs as keys, and metrics for each cluster like the count, sum of distances to centroid,
   *  and sum of squared distances
   */
  JavaPairRDD<Integer,ClusterMetric> fetchClusterMetrics(JavaRDD<Vector> evalData) {
    return evalData.mapToPair(new PairFunction<Vector,Integer,ClusterMetric>() {
      @Override
      public Tuple2<Integer,ClusterMetric> call(Vector vector) {
        double closestDist = Double.POSITIVE_INFINITY;
        int minClusterID = Integer.MIN_VALUE;
        double[] vec = vector.toArray();
        for (ClusterInfo cluster : clusters.values()) {
          double distance = distanceFn.distance(cluster.getCenter(), vec);
          if (distance < closestDist) {
            closestDist = distance;
            minClusterID = cluster.getID();
          }
        }
        Preconditions.checkState(!Double.isInfinite(closestDist) && !Double.isNaN(closestDist));
        return new Tuple2<>(minClusterID, new ClusterMetric(1L, closestDist, closestDist * closestDist));
      }
    }).reduceByKey(new Function2<ClusterMetric,ClusterMetric,ClusterMetric>() {
      @Override
      public ClusterMetric call(ClusterMetric a, ClusterMetric b) {
        return a.add(b);
      }
    });
  }

}
