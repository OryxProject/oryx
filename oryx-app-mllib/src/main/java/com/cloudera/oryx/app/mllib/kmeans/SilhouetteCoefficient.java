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
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import com.cloudera.oryx.app.kmeans.ClusterInfo;
import com.cloudera.oryx.app.kmeans.DistanceFn;

final class SilhouetteCoefficient extends AbstractKMeansEvaluation {

  private static final Logger log = LoggerFactory.getLogger(SilhouetteCoefficient.class);
  private static final long MAX_SAMPLE_SIZE = 100000;

  SilhouetteCoefficient(List<ClusterInfo> clusters) {
    super(clusters);
  }

  @Override
  double evaluate(JavaRDD<Vector> evalData) {
    JavaRDD<Vector> sampleData = fetchSampleData(evalData);
    return silhouetteCoefficient(fetchClusteredPoints(sampleData).collectAsMap());
  }

  /**
   * Computes the Silhouette Coefficient for this clustering, range is [-1,1]
   * (http://www.sciencedirect.com/science/article/pii/0377042787901257)
   * Here we are computing the overall Silhouette Coefficient across all of the clustering
   * Another option is to compute the Silhouette coefficient for each of the individual clusters
   * @param clusteredPointsMap - Map of <ClusterId, List<Points>> for whom Silhouette coefficient
   *                           needs to be computed
   * @return Silhouette Coefficient, closer to 1 is better
   */
  double silhouetteCoefficient(Map<Integer,Iterable<double[]>> clusteredPointsMap) {
    double overallSilhouetteCoefficientForClustering = 0.0;
    long sampleCount = 0L;

    for (Map.Entry<Integer, Iterable<double[]>> entry : clusteredPointsMap.entrySet()) {
      List<double[]> clusteredPoints = Lists.newArrayList(entry.getValue());
      // No. of points in this cluster
      long clusterSize = clusteredPoints.size();
      // Increment the total sample count for computing silhouette coefficient
      sampleCount += clusterSize;
      // if there's only one element in a cluster, then assume the silhouetteCoefficient for
      // the cluster = 0, this is an arbitrary choice per Section 2: Construction of Silhouettes
      // in the referenced paper
      if (clusterSize > 1) {
        double silhouetteCoefficientForCluster = 0.0;
        for (double[] point : clusteredPoints) {
          double pointIntraClusterDissimilarity =
              clusterDissimilarityForPoint(point, clusteredPoints, true);
          double pointInterClusterDissimilarity =
              minInterClusterDissimilarityForPoint(entry.getKey(), point, clusteredPointsMap);

          silhouetteCoefficientForCluster += calcSilhouetteCoefficient(
              pointIntraClusterDissimilarity, pointInterClusterDissimilarity);
        }
        overallSilhouetteCoefficientForClustering += silhouetteCoefficientForCluster;
      }

    }

    double silhouetteCoefficient = overallSilhouetteCoefficientForClustering / sampleCount;
    log.info("Computed Silhouette Coefficient for {} clusters: {}",
             getNumClusters(), silhouetteCoefficient);
    return silhouetteCoefficient;
  }

  static JavaRDD<Vector> fetchSampleData(JavaRDD<Vector> evalData) {
    JavaRDD<Vector> data = evalData;

    if (evalData.count() > MAX_SAMPLE_SIZE) {
      data = evalData.sample(false, (double) MAX_SAMPLE_SIZE / evalData.count());
    }
    return data;
  }

  private JavaPairRDD<Integer, Iterable<double[]>> fetchClusteredPoints(JavaRDD<Vector> evalData) {

    return evalData.mapToPair(new PairFunction<Vector, Integer, double[]>() {
      @Override
      public Tuple2<Integer, double[]> call(Vector vector) {
        double closestDist = Double.POSITIVE_INFINITY;
        int minCluster = -1;
        double[] vec = vector.toArray();

        List<ClusterInfo> clusters = getClusters();
        int numClusters = getNumClusters();
        DistanceFn<double[]> distanceFn = getDistanceFn();
        for (int i = 0; i < numClusters; i++) {
          ClusterInfo cluster = clusters.get(i);
          double distance = distanceFn.distance(cluster.getCenter(), vec);
          if (distance < closestDist) {
            closestDist = distance;
            minCluster = i;
          }
        }
        Preconditions.checkState(minCluster >= 0);
        return new Tuple2<>(minCluster, vec);
      }
    }).groupByKey();
  }

  private double clusterDissimilarityForPoint(double[] vector,
                                              List<double[]> points,
                                              boolean ownCluster) {
    DistanceFn<double[]> distanceFn = getDistanceFn();
    double totalDissimilarity = 0.0;
    for (double[] point : points) {
      totalDissimilarity += distanceFn.distance(vector, point);
    }

    if (ownCluster) {
      // (points.size -1) because a point's dissimilarity is being measured with other
      // points in its own cluster, hence there would be (n - 1) dissimilarities computed
      return totalDissimilarity / (points.size() - 1);
    } else {
      // point dissimilarity is being measured with all points of one of other clusters
      return totalDissimilarity / points.size();
    }
  }

  private double minInterClusterDissimilarityForPoint(
      int parentClusterId,
      double[] vector,
      Map<Integer, Iterable<double[]>> clusteredPointsMap) {

    double minInterClusterDissimilarity = Double.POSITIVE_INFINITY;

    for (Map.Entry<Integer, Iterable<double[]>> entry : clusteredPointsMap.entrySet()) {
      List<double[]> clusteredPoints = Lists.newArrayList(entry.getValue());
      // only compute dissimilarities with other clusters
      if (!entry.getKey().equals(parentClusterId)) {
        double otherClusterDissimilarity =
            clusterDissimilarityForPoint(vector, clusteredPoints, false);
        if (otherClusterDissimilarity < minInterClusterDissimilarity) {
          minInterClusterDissimilarity = otherClusterDissimilarity;
        }
      }
    }
    return minInterClusterDissimilarity;
  }

  static double calcSilhouetteCoefficient(double d1, double d2) {
    if (d1 < d2) {
      return 1.0 - (d1 / d2);
    }
    if (d1 > d2) {
      return (d2 / d1) - 1.0;
    }
    return 0.0;
  }
}
