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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import com.cloudera.oryx.app.kmeans.ClusterInfo;

public final class SilhouetteCoefficientTest extends AbstractKMeansEvalTest {

  private static final Logger log = LoggerFactory.getLogger(SilhouetteCoefficientTest.class);

  @Test
  public void testSilhouetteCoefficientForClustering() {
    List<ClusterInfo> clusters = getClusters();
    SilhouetteCoefficient silhouetteCoefficient = new SilhouetteCoefficient(clusters);

    Map<Integer, Iterable<double[]>> pointsMap = new HashMap<>();

    for (double[] point : POINTS) {
      Tuple2<Integer, Double> pointAssignedToCluster = pointClusterAssign(clusters, point);
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

    double eval = silhouetteCoefficient.silhouetteCoefficient(pointsMap);
    assertEquals(NUM_CLUSTERS, clusters.size());
    log.info("Silhouette Coefficient for {} clusters: {}", clusters.size(), eval);
    assertEquals(0.48484126984126985, eval);
  }
}
