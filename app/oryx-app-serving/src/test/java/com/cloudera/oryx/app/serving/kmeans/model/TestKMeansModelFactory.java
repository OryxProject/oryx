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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;

import com.cloudera.oryx.app.kmeans.ClusterInfo;
import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.common.settings.ConfigUtils;

public final class TestKMeansModelFactory {

  private TestKMeansModelFactory() {}

  public static KMeansServingModel buildTestModel() {
    List<ClusterInfo> clusters = new ArrayList<>();
    clusters.add(new ClusterInfo(2, new double[] { 1.0, 0.0 }, 1));
    clusters.add(new ClusterInfo(3, new double[] { 2.0, -1.0 }, 1));
    clusters.add(new ClusterInfo(4, new double[] { -1.0, 0.0 }, 1));

    Map<String,Object> overlayConfig = new HashMap<>();
    overlayConfig.put("oryx.input-schema.num-features", 2);
    overlayConfig.put("oryx.input-schema.categorical-features", "[]");
    Config config = ConfigUtils.overlayOn(overlayConfig, ConfigUtils.getDefault());
    InputSchema inputSchema = new InputSchema(config);

    return new KMeansServingModel(clusters, inputSchema);
  }

}
