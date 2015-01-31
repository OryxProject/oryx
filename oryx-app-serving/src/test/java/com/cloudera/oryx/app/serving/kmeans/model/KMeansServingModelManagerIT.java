/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
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

package com.cloudera.oryx.app.serving.kmeans.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.app.kmeans.ClusterInfo;
import com.cloudera.oryx.app.serving.AbstractOryxResource;
import com.cloudera.oryx.app.speed.kmeans.MockKMeansModelGenerator;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.lambda.serving.AbstractServingIT;

public final class KMeansServingModelManagerIT extends AbstractServingIT {

  private static final Logger log = LoggerFactory.getLogger(KMeansServingModelManagerIT.class);

  @Test
  public void testKMeansServingModel() throws Exception {
    Map<String,Object> overlayConfig = new HashMap<>();
    overlayConfig.put("oryx.serving.application-resources",
                      "\"com.cloudera.oryx.app.serving,com.cloudera.oryx.app.serving.kmeans\"");
    overlayConfig.put("oryx.serving.model-manager-class",
                      KMeansServingModelManager.class.getName());
    overlayConfig.put("oryx.input-schema.feature-names", "[\"x\",\"y\"]");
    overlayConfig.put("oryx.input-schema.categorical-features", "[]");
    Config config = ConfigUtils.overlayOn(overlayConfig, getConfig());

    startMessaging();
    startServer(config);
    startUpdateTopics(new MockKMeansModelGenerator(), 5);

    // Let updates finish
    Thread.sleep(1000);

    KMeansServingModelManager manager = (KMeansServingModelManager)
        getServingLayer().getContext().getServletContext().getAttribute(
            AbstractOryxResource.MODEL_MANAGER_KEY);

    assertNotNull("Manager must initialize in web context", manager);

    KMeansServingModel model = manager.getModel();
    log.debug("{}", model);

    List<ClusterInfo> clusters = model.getClusters();
    log.info("Clusters: {}", clusters);

    assertEquals(3, clusters.size());
    assertCluster(clusters.get(0), 0, new double[] { 2.0, 1.5 }, 2);
    assertCluster(clusters.get(1), 1, new double[] { 2.25, 0.75 }, 4);
    assertCluster(clusters.get(2), 2, new double[] { -0.25, 0.5 }, 4);
  }


  private static void assertCluster(ClusterInfo info,
                                    int expectedID,
                                    double[] expectedCenter,
                                    long expectedCount) {
    assertEquals(expectedID, info.getID());
    assertArrayEquals(expectedCenter, info.getCenter());
    assertEquals(expectedCount, info.getCount());
  }

}
