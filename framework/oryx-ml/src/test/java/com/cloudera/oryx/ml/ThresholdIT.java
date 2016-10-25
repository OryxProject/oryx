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

package com.cloudera.oryx.ml;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import com.typesafe.config.Config;
import org.junit.Test;

import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.lambda.batch.AbstractBatchIT;

/**
 * Tests {@link MLUpdate} threshold, where model doesn't pass threshold.
 */
public final class ThresholdIT extends AbstractBatchIT {

  private static final int DATA_TO_WRITE = 50;
  private static final int WRITE_INTERVAL_MSEC = 25;
  private static final int GEN_INTERVAL_SEC = 2;

  @Test
  public void testMLUpdate() throws Exception {
    Path tempDir = getTempDir();
    Path modelDir = tempDir.resolve("model");
    Map<String,Object> overlayConfig = new HashMap<>();
    overlayConfig.put("oryx.batch.update-class", MockMLUpdate.class.getName());
    ConfigUtils.set(overlayConfig, "oryx.batch.storage.data-dir", tempDir.resolve("data"));
    ConfigUtils.set(overlayConfig, "oryx.batch.storage.model-dir", modelDir);
    overlayConfig.put("oryx.batch.streaming.generation-interval-sec", GEN_INTERVAL_SEC);
    overlayConfig.put("oryx.ml.eval.threshold", DATA_TO_WRITE * 2); // Won't pass
    Config config = ConfigUtils.overlayOn(overlayConfig, getConfig());

    startMessaging();
    startServerProduceConsumeTopics(config, DATA_TO_WRITE, WRITE_INTERVAL_MSEC);
    assertTrue(IOUtils.listFiles(modelDir, "*").isEmpty());
  }

}
