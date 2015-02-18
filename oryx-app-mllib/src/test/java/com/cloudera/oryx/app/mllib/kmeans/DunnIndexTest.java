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
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import com.cloudera.oryx.app.kmeans.ClusterInfo;

public final class DunnIndexTest extends AbstractKMeansEvalTest {

  private static final Logger log = LoggerFactory.getLogger(DunnIndexTest.class);

  @Test
  public void testDunnIndexForClustering() {
    List<ClusterInfo> clusters = getClusters();
    DunnIndex dunnIndex = new DunnIndex(clusters);
    List<Tuple2<Integer, Tuple2<Double, Long>>> clustersList =
        fetchClusterSumDistanceAndCountsAsList(clusters, POINTS);

    double eval = dunnIndex.dunnIndex(clustersList);
    assertEquals(NUM_CLUSTERS, clusters.size());
    log.info("Dunn Index for {} clusters: {}", clusters.size(), eval);
    assertEquals(1.7142857142857142, eval);
  }

  private static List<Tuple2<Integer, Tuple2<Double, Long>>> fetchClusterSumDistanceAndCountsAsList(
      List<ClusterInfo> clusters,
      List<double[]> points) {

    Map<Integer, Tuple2<Double, Long>> clusterSumDistAndCountsMap =
        fetchClusterSumDistanceAndCounts(clusters, points);
    List<Tuple2<Integer, Tuple2<Double, Long>>> clustersList = new ArrayList<>();

    for (int i = 0; i < clusters.size(); i++) {
      clustersList.add(new Tuple2<>(i, clusterSumDistAndCountsMap.get(i)));
    }

    return clustersList;
  }
}
