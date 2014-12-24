/*
 * Copyright (c) 2014, Cloudera and Intel, Inc. All Rights Reserved.
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

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.settings.ConfigUtils;

public final class KMeansUpdateIT extends AbstractKMeansIT {

  private static final Logger log = LoggerFactory.getLogger(KMeansUpdateIT.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final int DATA_TO_WRITE = 2000;
  private static final int WRITE_INTERVAL_MSEC = 10;
  private static final int GEN_INTERVAL_SEC = 10;
  private static final int BLOCK_INTERVAL_SEC = 1;
  private static final int CLUSTERS = 5;

  @Test
  public void testALS() throws Exception {
    Path tempDir = getTempDir();
    Path dataDir = tempDir.resolve("data");
    Path modelDir = tempDir.resolve("model");

    Map<String,Object> overlayConfig = new HashMap<>();
    overlayConfig.put("oryx.batch.update-class", KMeansUpdate.class.getName());
    ConfigUtils.set(overlayConfig, "oryx.batch.storage.data-dir", dataDir);
    ConfigUtils.set(overlayConfig, "oryx.batch.storage.model-dir", modelDir);
    overlayConfig.put("oryx.batch.streaming.generation-interval-sec", GEN_INTERVAL_SEC);
    overlayConfig.put("oryx.batch.streaming.block-interval-sec", BLOCK_INTERVAL_SEC);
    overlayConfig.put("oryx.kmeans.hyperparams.k", CLUSTERS);
    Config config = ConfigUtils.overlayOn(overlayConfig, getConfig());

    // TODO: Add the real test when implementation is done
  }

}
