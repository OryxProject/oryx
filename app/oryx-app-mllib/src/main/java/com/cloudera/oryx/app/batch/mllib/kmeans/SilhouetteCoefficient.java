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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple2;

import com.cloudera.oryx.app.kmeans.ClusterInfo;
import com.cloudera.oryx.app.kmeans.DistanceFn;

final class SilhouetteCoefficient extends AbstractKMeansEvaluation {

  private static final long MAX_SAMPLE_SIZE = 100000;

  SilhouetteCoefficient(Collection<ClusterInfo> clusters) {
    super(clusters);
  }

  @Override
  double evaluate(JavaRDD<Vector> evalData) {
    return silhouetteCoefficient(fetchClusteredPoints(fetchSampleData(evalData)).collectAsMap());
  }

  /**
   * Computes the Silhouette Coefficient for this clustering.
   * See https://en.wikipedia.org/wiki/Silhouette_(clustering)
   *
   * @param clusterPointsByID map of cluster ID to points that map to that cluster,
   *  for whom Silhouette coefficient needs to be computed
   * @return Silhouette Coefficient in range [-1,1]; higher is better
   */
  double silhouetteCoefficient(Map<Integer,Iterable<double[]>> clusterPointsByID) {
    double totalSilhouetteCoefficient = 0.0;
    long sampleCount = 0L;

    for (Map.Entry<Integer, Iterable<double[]>> entry : clusterPointsByID.entrySet()) {
      List<double[]> clusteredPoints = iterableToList(entry.getValue());
      long clusterSize = clusteredPoints.size();
      // Increment the total sample count for computing silhouette coefficient
      sampleCount += clusterSize;
      // if there's only one element in a cluster, then assume the silhouetteCoefficient for
      // the cluster = 0, this is an arbitrary choice per Section 2: Construction of Silhouettes
      // in the referenced paper
      if (clusterSize > 1) {
        for (double[] point : clusteredPoints) {
          double pointIntraClusterDissimilarity = clusterDissimilarityForPoint(point, clusteredPoints, true);
          double pointInterClusterDissimilarity =
              minInterClusterDissimilarityForPoint(entry.getKey(), point, clusterPointsByID);
          totalSilhouetteCoefficient += silhouetteCoefficient(
              pointIntraClusterDissimilarity, pointInterClusterDissimilarity);
        }
      }

    }

    return sampleCount == 0 ? 0.0 : totalSilhouetteCoefficient / sampleCount;
  }

  static JavaRDD<Vector> fetchSampleData(JavaRDD<Vector> evalData) {
    long count = evalData.count();
    if (count > MAX_SAMPLE_SIZE) {
      return evalData.sample(false, (double) MAX_SAMPLE_SIZE / count);
    }
    return evalData;
  }

  private JavaPairRDD<Integer, Iterable<double[]>> fetchClusteredPoints(JavaRDD<? extends Vector> evalData) {
    return evalData.mapToPair(vector -> {
      double closestDist = Double.POSITIVE_INFINITY;
      int minClusterID = Integer.MIN_VALUE;
      double[] vec = vector.toArray();
      DistanceFn<double[]> distanceFn = getDistanceFn();
      Map<Integer,ClusterInfo> clusters = getClustersByID();
      for (ClusterInfo cluster : clusters.values()) {
        double distance = distanceFn.applyAsDouble(cluster.getCenter(), vec);
        if (distance < closestDist) {
          closestDist = distance;
          minClusterID = cluster.getID();
        }
      }
      Preconditions.checkState(!Double.isInfinite(closestDist) && !Double.isNaN(closestDist));
      return new Tuple2<>(minClusterID, vec);
    }).groupByKey();
  }

  private double clusterDissimilarityForPoint(double[] point,
                                              List<double[]> clusterPoints,
                                              boolean ownCluster) {
    DistanceFn<double[]> distanceFn = getDistanceFn();
    double totalDissimilarity = 0.0;
    for (double[] clusterPoint : clusterPoints) {
      totalDissimilarity += distanceFn.applyAsDouble(point, clusterPoint);
    }

    if (ownCluster) {
      // (points.size -1) because a point's dissimilarity is being measured with other
      // points in its own cluster, hence there would be (n - 1) dissimilarities computed
      return totalDissimilarity / (clusterPoints.size() - 1);
    } else {
      // point dissimilarity is being measured with all points of one of other clusters
      return totalDissimilarity / clusterPoints.size();
    }
  }

  private double minInterClusterDissimilarityForPoint(
      int otherClusterID,
      double[] point,
      Map<Integer, Iterable<double[]>> clusteredPointsMap) {
    return clusteredPointsMap.entrySet().stream().mapToDouble(entry -> {
      // only compute dissimilarities with other clusters
      if (entry.getKey().equals(otherClusterID)) {
        return Double.POSITIVE_INFINITY;
      }
      return clusterDissimilarityForPoint(point, iterableToList(entry.getValue()), false);
    }).min().orElse(Double.POSITIVE_INFINITY);
  }

  static double silhouetteCoefficient(double ai, double bi) {
    if (ai < bi) {
      return 1.0 - (ai / bi);
    }
    if (ai > bi) {
      return (bi / ai) - 1.0;
    }
    return 0.0;
  }

  private static <T> List<T> iterableToList(Iterable<T> it) {
    return it instanceof List ? (List<T>) it : Lists.newArrayList(it);
  }

}
