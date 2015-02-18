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
import org.dmg.pmml.Cluster;
import org.dmg.pmml.ClusteringModel;
import org.dmg.pmml.Model;
import org.dmg.pmml.PMML;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.collection.Pair;
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
    overlayConfig.put("oryx.speed.streaming.generation-interval-sec", 10);
    overlayConfig.put("oryx.speed.streaming.block-interval-sec", 1);
    overlayConfig.put("oryx.input-schema.feature-names", "[\"x\",\"y\"]");
    overlayConfig.put("oryx.input-schema.categorical-features", "[]");
    Config config = ConfigUtils.overlayOn(overlayConfig, getConfig());

    startMessaging();

    List<Pair<String,String>> updates =
        startServerProduceConsumeTopics(config,
                                        new MockKMeansInputGenerator(),
                                        new MockKMeansModelGenerator(),
                                        300, // @10 msec should make 1 interval
                                        1);

    for (Pair<String, String> update : updates) {
      log.info("{}", update);
    }

    int numUpdates = updates.size();

    assertEquals(NUM_CLUSTERS + 1, updates.size());
    assertEquals("MODEL", updates.get(0).getFirst());

    PMML pmml = PMMLUtils.fromString(updates.get(0).getSecond());
    Model model = pmml.getModels().get(0);
    assertTrue(model instanceof ClusteringModel);

    ClusteringModel clusteringModel = (ClusteringModel) model;
    assertEquals(NUM_CLUSTERS, clusteringModel.getNumberOfClusters().intValue());
    List<Cluster> clusters = clusteringModel.getClusters();

    for (int i = 1; i < numUpdates; i++) {
      Pair<String,String> update = updates.get(i);
      assertEquals("UP", update.getFirst());
      List<?> fields = MAPPER.readValue(update.getSecond(), List.class);
      int clusterID = (Integer) fields.get(0);

      double[] updatedCenter = MAPPER.convertValue(fields.get(1), double[].class);
      Cluster cluster = clusters.get(clusterID);

      String[] tokens = TextUtils.parseDelimited(cluster.getArray().getValue(), ' ');
      double[] modelCenter = VectorMath.parseVector(tokens);

      assertEquals(tokens.length, modelCenter.length);
      assertFalse(Arrays.equals(modelCenter, updatedCenter));
      // Should be heavily weighted now towards the update point
      assertArrayEquals(updatedCenter, MockKMeansInputGenerator.UPDATE_POINTS[i - 1], 0.1);

      int updatedClusterSize = (Integer) fields.get(2);
      assertTrue(updatedClusterSize > cluster.getSize());
      assertEquals(100 + cluster.getSize(), updatedClusterSize);
    }
  }

}
