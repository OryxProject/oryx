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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;
import org.dmg.pmml.PMML;
import org.junit.Test;

import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.settings.ConfigUtils;

public final class KMeansPMMLUtilsTest extends OryxTest {

  private static final int NUM_CLUSTERS = 3;
  private static final int CLUSTER_SIZE = 2;
  private static final int NUM_DIMENSIONS = 2;

  @Test
  public void testClustering() {
    PMML pmml = KMeansPMMLUtils.buildDummyClusteringModel();
    Map<String,Object> overlayConfig = new HashMap<>();
    overlayConfig.put("oryx.input-schema.feature-names", "[\"x\",\"y\"]");
    overlayConfig.put("oryx.input-schema.num-features", 2);
    overlayConfig.put("oryx.input-schema.categorical-features", "[]");
    Config config = ConfigUtils.overlayOn(overlayConfig, ConfigUtils.getDefault());
    InputSchema schema = new InputSchema(config);
    KMeansPMMLUtils.validatePMMLVsSchema(pmml, schema);
  }

  @Test
  public void testPMMLToClusters() {
    PMML pmml = KMeansPMMLUtils.buildDummyClusteringModel();
    List<ClusterInfo> clusterInfoList = KMeansPMMLUtils.read(pmml);
    assertEquals(NUM_CLUSTERS, clusterInfoList.size());
    assertEquals(NUM_DIMENSIONS, clusterInfoList.get(0).getCenter().length);
    assertEquals(CLUSTER_SIZE, clusterInfoList.get(1).getCount());
  }

}
