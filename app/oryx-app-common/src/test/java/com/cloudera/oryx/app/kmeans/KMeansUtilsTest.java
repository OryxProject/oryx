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

package com.cloudera.oryx.app.kmeans;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;
import org.junit.Test;

import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.settings.ConfigUtils;

public final class KMeansUtilsTest extends OryxTest {

  private static final DistanceFn<double[]> DISTANCE_FN = new EuclideanDistanceFn();

  @Test(expected = IllegalArgumentException.class)
  public void testNoClusters() {
    KMeansUtils.closestCluster(Collections.emptyList(), DISTANCE_FN, new double[] { 1.0 });
  }

  @Test
  public void testClosestCluster() {
    List<ClusterInfo> clusters = Arrays.asList(
        new ClusterInfo(2, new double[] { 1.0,  2.0 }, 1),
        new ClusterInfo(4, new double[] { 0.0, -2.0 }, 1),
        new ClusterInfo(1, new double[] { 3.0,  1.0 }, 1)
    );
    Pair<ClusterInfo,Double> closestPairA =
        KMeansUtils.closestCluster(clusters, DISTANCE_FN, new double[] { 0.0, -2.0 });
    assertEquals(4, closestPairA.getFirst().getID());
    assertEquals(0.0, closestPairA.getSecond().doubleValue());
    Pair<ClusterInfo,Double> closestPairB =
        KMeansUtils.closestCluster(clusters, DISTANCE_FN, new double[] { 6.0, 5.0 });
    assertEquals(1, closestPairB.getFirst().getID());
    assertEquals(5.0, closestPairB.getSecond().doubleValue());
  }

  @Test
  public void testFeaturesFromTokens() {
    Map<String,Object> overlayConfig = new HashMap<>();
    overlayConfig.put("oryx.input-schema.num-features", 4);
    overlayConfig.put("oryx.input-schema.categorical-features", "[]");
    overlayConfig.put("oryx.input-schema.ignored-features", "[0,2]");
    Config config = ConfigUtils.overlayOn(overlayConfig, ConfigUtils.getDefault());
    InputSchema inputSchema = new InputSchema(config);
    double[] features = KMeansUtils.featuresFromTokens(new String[] { "1.0", "2.0", "0.0", "-3.5" }, inputSchema);
    assertArrayEquals(new double[] { 2.0, -3.5 }, features);
  }

  @Test
  public void testUniqueIDs() {
    KMeansUtils.checkUniqueIDs(Arrays.asList(
        new ClusterInfo(2, new double[] { 1.0 }, 1),
        new ClusterInfo(4, new double[] { 0.0 }, 1),
        new ClusterInfo(1, new double[] { 3.0 }, 1)
    ));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNonUniqueIDs() {
    KMeansUtils.checkUniqueIDs(Arrays.asList(
        new ClusterInfo(2, new double[] { 1.0 }, 1),
        new ClusterInfo(4, new double[] { 0.0 }, 1),
        new ClusterInfo(2, new double[] { 3.0 }, 1)
    ));
  }

}
