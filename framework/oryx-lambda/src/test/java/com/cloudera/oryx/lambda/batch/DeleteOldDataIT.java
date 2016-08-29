/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
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

package com.cloudera.oryx.lambda.batch;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import com.typesafe.config.Config;
import org.junit.Test;

import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;

/**
 * Tests the effect of {@link com.cloudera.oryx.lambda.DeleteOldDataFn}.
 */
public final class DeleteOldDataIT extends AbstractBatchIT {

  private static final int DATA_TO_WRITE = 600;
  private static final int WRITE_INTERVAL_MSEC = 20;
  private static final int GEN_INTERVAL_SEC = 3;

  @Test
  public void testDeleteOldData() throws Exception {
    Path tempDir = getTempDir();
    Path dataDir = tempDir.resolve("data");
    Path modelDir = tempDir.resolve("model");
    Map<String,Object> overlayConfig = new HashMap<>();
    overlayConfig.put("oryx.batch.update-class", MockBatchUpdate.class.getName());
    ConfigUtils.set(overlayConfig, "oryx.batch.storage.data-dir", dataDir);
    ConfigUtils.set(overlayConfig, "oryx.batch.storage.model-dir", modelDir);
    overlayConfig.put("oryx.batch.storage.max-age-data-hours", 0);
    overlayConfig.put("oryx.batch.storage.max-age-model-hours", 0);
    overlayConfig.put("oryx.batch.streaming.generation-interval-sec", GEN_INTERVAL_SEC);
    Config config = ConfigUtils.overlayOn(overlayConfig, getConfig());

    startMessaging();
    startServerProduceConsumeTopics(config, DATA_TO_WRITE, WRITE_INTERVAL_MSEC);
    assertEquals(0, IOUtils.listFiles(dataDir, "*").size());
    assertEquals(0, IOUtils.listFiles(modelDir, "*").size());
  }

}
