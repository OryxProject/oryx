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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import com.cloudera.oryx.app.kmeans.ClusterInfo;

public final class DaviesBouldinIndexTest extends AbstractKMeansEvalTest {

  private static final Logger log = LoggerFactory.getLogger(DaviesBouldinIndexTest.class);

  @Test
  public void testDaviesBouldinIndexForClustering() {
    List<ClusterInfo> clusters = getClusters();
    DaviesBouldinIndex daviesBouldinIndex = new DaviesBouldinIndex(clusters);
    Map<Integer, Tuple2<Double, Long>> clusterSumDistancesAndCounts =
        fetchClusterSumDistanceAndCounts(clusters, POINTS);

    double eval = daviesBouldinIndex.daviesBouldinIndex(clusterSumDistancesAndCounts);
    assertEquals(NUM_CLUSTERS, clusters.size());
    log.info("Davies Bouldin Index for {} clusters: {}", clusters.size(), eval);
    assertEquals(0.638888888888889, eval);
  }

}
