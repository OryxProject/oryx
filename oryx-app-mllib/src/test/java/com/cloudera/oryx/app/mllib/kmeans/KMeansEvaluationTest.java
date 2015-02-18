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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import org.dmg.pmml.PMML;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import com.cloudera.oryx.app.kmeans.ClusterInfo;
import com.cloudera.oryx.app.kmeans.DistanceFn;
import com.cloudera.oryx.app.kmeans.KMeansPMMLUtils;
import com.cloudera.oryx.app.kmeans.SquaredDistanceFn;
import com.cloudera.oryx.common.OryxTest;

public final class KMeansEvaluationTest extends OryxTest {

  private final static Logger log = LoggerFactory.getLogger(KMeansEvaluationTest.class);

  private static final int NUM_CLUSTERS = 3;

  private static final List<double[]> points =
      Arrays.asList(new double[][]{
          {1.0, 0.0}, {2.0, -2.0}, {2.0, 0.0},
          {-2.0, 0.0}, {-0.5, -1.0}, {-0.5, 1.0}
      });

  private static List<ClusterInfo> clusters;
  private static DistanceFn<double[]> distanceFn = new SquaredDistanceFn();

  @Test
  public void testDaviesBouldinIndexForClustering() {
    KMeansEvaluation kMeansEvaluation = getKMeansEvaluationCriteria();
    Map<Integer, Tuple2<Double, Long>> clusterSumDistancesAndCounts =
        fetchClusterSumDistanceAndCounts(points);

    double daviesBouldinIndex = kMeansEvaluation.daviesBouldinIndex(clusterSumDistancesAndCounts);
    assertEquals(NUM_CLUSTERS, clusters.size());
    log.info("Davies Bouldin Index for {} clusters: {}", clusters.size(), daviesBouldinIndex);
    assertEquals(0.638888888888889, daviesBouldinIndex);
  }

  @Test
  public void testDunnIndexForClustering() {
    KMeansEvaluation kMeansEvaluation = getKMeansEvaluationCriteria();
    List<Tuple2<Integer, Tuple2<Double, Long>>> clustersList =
        fetchClusterSumDistanceAndCountsAsList(clusters, points);

    double dunnIndex = kMeansEvaluation.dunnIndex(clustersList);
    assertEquals(NUM_CLUSTERS, clusters.size());
    log.info("Dunn Index for {} clusters: {}", clusters.size(), dunnIndex);
    assertEquals(1.7142857142857142, dunnIndex);
  }

  @Test
  public void testSilhouetteCoefficientForClustering() {
    KMeansEvaluation kMeansEvaluation = getKMeansEvaluationCriteria();
    Map<Integer, Iterable<double[]>> pointsMap = new HashMap<>();

    for (double[] point : points) {
      Tuple2<Integer, Double> pointAssignedToCluster = pointClusterAssign(point);
      int clusterId = pointAssignedToCluster._1();
      List<double[]> pointsToClusterList;

      if (pointsMap.isEmpty() || !pointsMap.containsKey(clusterId)) {
        pointsToClusterList = new ArrayList<>();
        pointsToClusterList.add(point);
        pointsMap.put(clusterId, pointsToClusterList);
      } else {
        pointsToClusterList = Lists.newArrayList(pointsMap.get(clusterId));
        pointsToClusterList.add(point);
        pointsMap.put(clusterId, pointsToClusterList);
      }
    }

    double silhouetteCoefficient = kMeansEvaluation.silhouetteCoefficient(pointsMap);
    assertEquals(NUM_CLUSTERS, clusters.size());
    log.info("Silhouette Coefficient for {} clusters: {}", clusters.size(), silhouetteCoefficient);
    assertEquals(0.48484126984126985, silhouetteCoefficient);
  }

  private static KMeansEvaluation getKMeansEvaluationCriteria() {
    PMML pmml = KMeansPMMLUtils.buildDummyClusteringModel();
    clusters = KMeansPMMLUtils.read(pmml);
    return new KMeansEvaluation(clusters);
  }

  private static Map<Integer, Tuple2<Double, Long>> fetchClusterSumDistanceAndCounts(
      List<double[]> points) {

    Map<Integer, Tuple2<Double, Long>> clusterSumDistAndCountsMap = new HashMap<>();

    for (double[] point : points) {
      Tuple2<Integer, Double> clusterAssign = pointClusterAssign(point);
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

  private static List<Tuple2<Integer, Tuple2<Double, Long>>> fetchClusterSumDistanceAndCountsAsList(
      List<ClusterInfo> clusters,
      List<double[]> points) {

    Map<Integer, Tuple2<Double, Long>> clusterSumDistAndCountsMap =
        fetchClusterSumDistanceAndCounts(points);
    List<Tuple2<Integer, Tuple2<Double, Long>>> clustersList = new ArrayList<>();

    for (int i = 0; i < clusters.size(); i++) {
      clustersList.add(new Tuple2<>(i, clusterSumDistAndCountsMap.get(i)));
    }

    return clustersList;
  }

  private static Tuple2<Integer, Double> pointClusterAssign(double[] point) {
    double closestDist = Double.POSITIVE_INFINITY;
    int minCluster = -1;

    for (int i = 0; i < clusters.size(); i++) {
      ClusterInfo cluster = clusters.get(i);
      double distance = distanceFn.distance(cluster.getCenter(), point);
      if (distance < closestDist) {
        closestDist = distance;
        minCluster = i;
      }
    }

    return new Tuple2<>(minCluster, closestDist);
  }

}
