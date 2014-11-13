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

package com.cloudera.oryx.ml.mllib.als;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;
import org.dmg.pmml.Extension;
import org.dmg.pmml.PMML;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.ml.MLUpdate;
import com.cloudera.oryx.common.pmml.PMMLUtils;

public final class HyperParamTuningIT extends AbstractALSIT {

  private static final Logger log = LoggerFactory.getLogger(HyperParamTuningIT.class);

  private static final int DATA_TO_WRITE = 2000;
  private static final int WRITE_INTERVAL_MSEC = 10;
  private static final int GEN_INTERVAL_SEC = 30;
  private static final int BLOCK_INTERVAL_SEC = 1;
  private static final int TEST_FEATURES = 7;
  private static final int TEST_ELEMENTS = 1000;

  @Test
  public void testALS() throws Exception {
    Path tempDir = getTempDir();
    Path dataDir =  tempDir.resolve("data");
    Path modelDir = tempDir.resolve("model");

    Map<String,String> overlayConfig = new HashMap<>();
    overlayConfig.put("batch.update-class", ALSUpdate.class.getName());
    ConfigUtils.set(overlayConfig, "batch.storage.data-dir", dataDir);
    ConfigUtils.set(overlayConfig, "batch.storage.model-dir", modelDir);
    overlayConfig.put("batch.generation-interval-sec",
                      Integer.toString(GEN_INTERVAL_SEC));
    overlayConfig.put("batch.block-interval-sec",
                      Integer.toString(BLOCK_INTERVAL_SEC));
    // Choose pairs of values where the best is predictable
    overlayConfig.put("als.hyperparams.features", "[1," + TEST_FEATURES + "]");
    overlayConfig.put("ml.eval.candidates", "2");
    overlayConfig.put("ml.eval.parallelism", "2");
    Config config = ConfigUtils.overlayOn(overlayConfig, getConfig());

    startMessageQueue();

    startServerProduceConsumeQueues(config,
                                    new FeaturesALSDataGenerator(TEST_ELEMENTS,
                                                                 TEST_ELEMENTS,
                                                                 TEST_FEATURES),
                                    DATA_TO_WRITE,
                                    WRITE_INTERVAL_MSEC);

    List<Path> modelInstanceDirs = IOUtils.listFiles(modelDir, "*");
    log.info("Model instance dirs: {}", modelInstanceDirs);
    assertFalse("No models?", modelInstanceDirs.isEmpty());

    checkIntervals(modelInstanceDirs.size(), DATA_TO_WRITE, WRITE_INTERVAL_MSEC, GEN_INTERVAL_SEC);

    Path modelFile = modelInstanceDirs.get(0).resolve(MLUpdate.MODEL_FILE_NAME);
    assertTrue("No such model file: " + modelFile, Files.exists(modelFile));

    PMML pmml = PMMLUtils.read(modelFile);
    List<Extension> extensions = pmml.getExtensions();
    assertEquals(8, extensions.size());
    assertNotNull(PMMLUtils.getExtensionValue(pmml, "X"));
    assertNotNull(PMMLUtils.getExtensionValue(pmml, "Y"));
    assertTrue(Boolean.parseBoolean(PMMLUtils.getExtensionValue(pmml, "implicit")));
    assertEquals(0.001, Double.parseDouble(PMMLUtils.getExtensionValue(pmml, "lambda")));
    assertEquals(1.0, Double.parseDouble(PMMLUtils.getExtensionValue(pmml, "alpha")));
    assertEquals(TEST_FEATURES, Integer.parseInt(PMMLUtils.getExtensionValue(pmml, "features")));
  }

}
