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
import com.google.common.collect.Lists;
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
  private static final long MAX_SAMPLE_SIZE = 100000;

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
    return daviesBouldinIndex(fetchClusterSumDistanceAndCounts(evalData).collectAsMap());
  }

  double daviesBouldinIndex(Map<Integer,Tuple2<Double,Long>> clusterSumDistAndCounts) {
    double totalDBIndex = 0.0;
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

  /**
   * Computes the Dunn Index of a given clustering
   * (http://en.wikipedia.org/wiki/Dunn_index)
   * @param evalData data for evaluation
   * @return Dunn Index; higher is better
   */
  double dunnIndex(JavaRDD<Vector> evalData) {
    return dunnIndex(fetchClusterSumDistanceAndCounts(evalData).collect());
  }

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

  /**
   * Computes the Silhouette Coefficient for this clustering, range is [-1,1]
   * (http://www.sciencedirect.com/science/article/pii/0377042787901257)
   * Here we are computing the overall Silhouette Coefficient across all of the clustering
   * Another option is to compute the Silhouette coefficient for each of the individual clusters
   * @param evalData data for evaluation
   * @return Silhouette Coefficient, closer to 1 is better
   */
  double silhouetteCoefficient(JavaRDD<Vector> evalData) {
    JavaRDD<Vector> sampleData = fetchSampleData(evalData);
    return silhouetteCoefficient(fetchClusteredPoints(sampleData).collectAsMap());
  }

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
    log.info("Computed Silhouette Coefficient for {} clusters: {}", numClusters, silhouetteCoefficient);
    return silhouetteCoefficient;
  }

  private static JavaRDD<Vector> fetchSampleData(JavaRDD<Vector> evalData) {
    JavaRDD<Vector> data = evalData;

    if (evalData.count() > MAX_SAMPLE_SIZE) {
      data = evalData.sample(false, (double) MAX_SAMPLE_SIZE / evalData.count());
    }
    return data;
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

  private JavaPairRDD<Integer, Iterable<double[]>> fetchClusteredPoints(JavaRDD<Vector> evalData) {

    return evalData.mapToPair(new PairFunction<Vector, Integer, double[]>() {
      @Override
      public Tuple2<Integer, double[]> call(Vector vector) {
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
        return new Tuple2<>(minCluster, vec);
      }
    }).groupByKey();
  }

  private double clusterDissimilarityForPoint(double[] vector,
                                              List<double[]> points,
                                              boolean ownCluster) {
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

  private static double calcSilhouetteCoefficient(double d1, double d2) {
    if (d1 < d2) {
      return 1.0 - (d1 / d2);
    }
    if (d1 > d2) {
      return (d2 / d1) - 1.0;
    }
    return 0.0;
  }

}