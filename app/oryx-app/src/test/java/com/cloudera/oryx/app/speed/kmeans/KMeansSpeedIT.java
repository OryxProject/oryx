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

package com.cloudera.oryx.app.speed.kmeans;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;
import org.dmg.pmml.Model;
import org.dmg.pmml.PMML;
import org.dmg.pmml.clustering.Cluster;
import org.dmg.pmml.clustering.ClusteringModel;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.api.KeyMessage;
import com.cloudera.oryx.app.kmeans.ClusterInfo;
import com.cloudera.oryx.common.math.VectorMath;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.common.text.TextUtils;
import com.cloudera.oryx.lambda.speed.AbstractSpeedIT;

public final class KMeansSpeedIT extends AbstractSpeedIT {

  private static final Logger log = LoggerFactory.getLogger(KMeansSpeedIT.class);

  private static final int NUM_CLUSTERS = 3;

  @Test
  public void testKMeansSpeed() throws Exception {
    Map<String,Object> overlayConfig = new HashMap<>();
    overlayConfig.put("oryx.speed.model-manager-class", KMeansSpeedModelManager.class.getName());
    overlayConfig.put("oryx.speed.streaming.generation-interval-sec", 6);
    overlayConfig.put("oryx.input-schema.feature-names", "[\"x\",\"y\"]");
    overlayConfig.put("oryx.input-schema.categorical-features", "[]");
    Config config = ConfigUtils.overlayOn(overlayConfig, getConfig());

    startMessaging();

    List<KeyMessage<String,String>> updates =
        startServerProduceConsumeTopics(config,
                                        new MockKMeansInputGenerator(),
                                        new MockKMeansModelGenerator(),
                                        300, // @10 msec should make 1 interval
                                        1);

    updates.forEach(update -> log.info("{}", update));

    int numUpdates = updates.size();

    // Model plus at least 3 updates, 1 per cluster
    assertGreaterOrEqual(updates.size(), NUM_CLUSTERS + 1);
    assertEquals("MODEL", updates.get(0).getKey());

    PMML pmml = PMMLUtils.fromString(updates.get(0).getMessage());
    Model model = pmml.getModels().get(0);
    assertInstanceOf(model, ClusteringModel.class);

    ClusteringModel clusteringModel = (ClusteringModel) model;
    assertEquals(NUM_CLUSTERS, clusteringModel.getNumberOfClusters());
    List<Cluster> clusters = clusteringModel.getClusters();

    Map<Integer,ClusterInfo> clusterInfos = new HashMap<>();
    for (int i = 1; i < numUpdates; i++) {
      KeyMessage<String,String> update = updates.get(i);
      assertEquals("UP", update.getKey());
      List<?> fields = TextUtils.readJSON(update.getMessage(), List.class);
      int clusterID = (Integer) fields.get(0);
      double[] updatedCenter = TextUtils.convertViaJSON(fields.get(1), double[].class);
      int updatedClusterSize = (Integer) fields.get(2);
      clusterInfos.put(clusterID, new ClusterInfo(clusterID, updatedCenter, updatedClusterSize));
    }

    assertEquals(3, clusterInfos.size());

    for (ClusterInfo clusterInfo : clusterInfos.values()) {
      int id = clusterInfo.getID();
      Cluster cluster = clusters.get(id);

      String[] tokens = TextUtils.parseDelimited(cluster.getArray().getValue(), ' ');
      double[] modelCenter = VectorMath.parseVector(tokens);

      double[] updatedCenter = clusterInfo.getCenter();
      assertEquals(tokens.length, modelCenter.length);
      assertFalse(Arrays.equals(modelCenter, updatedCenter));
      // Should be heavily weighted now towards the update point
      assertArrayEquals(updatedCenter, MockKMeansInputGenerator.UPDATE_POINTS[id], 0.1);

      long updatedClusterSize = clusterInfo.getCount();
      assertGreater(updatedClusterSize, cluster.getSize());
      assertEquals(100 + cluster.getSize(), updatedClusterSize);
    }
  }

}
