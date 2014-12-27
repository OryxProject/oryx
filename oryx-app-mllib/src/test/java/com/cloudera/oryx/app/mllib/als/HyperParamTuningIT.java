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

package com.cloudera.oryx.app.mllib.als;

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

import com.cloudera.oryx.app.pmml.AppPMMLUtils;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.ml.MLUpdate;

public final class HyperParamTuningIT extends AbstractALSIT {

  private static final Logger log = LoggerFactory.getLogger(HyperParamTuningIT.class);

  private static final int DATA_TO_WRITE = 10000;
  private static final int WRITE_INTERVAL_MSEC = 10;
  private static final int GEN_INTERVAL_SEC = 30;
  private static final int BLOCK_INTERVAL_SEC = 1;
  private static final int TEST_FEATURES = 7;
  private static final int TEST_ELEMENTS = 100;

  @Test
  public void testHyperParameterTuning() throws Exception {
    Path tempDir = getTempDir();
    Path dataDir =  tempDir.resolve("data");
    Path modelDir = tempDir.resolve("model");

    Map<String,Object> overlayConfig = new HashMap<>();
    overlayConfig.put("oryx.batch.update-class", ALSUpdate.class.getName());
    ConfigUtils.set(overlayConfig, "oryx.batch.storage.data-dir", dataDir);
    ConfigUtils.set(overlayConfig, "oryx.batch.storage.model-dir", modelDir);
    overlayConfig.put("oryx.batch.streaming.generation-interval-sec", GEN_INTERVAL_SEC);
    overlayConfig.put("oryx.batch.streaming.block-interval-sec", BLOCK_INTERVAL_SEC);
    // Choose pairs of values where the best is predictable
    overlayConfig.put("oryx.als.hyperparams.features", "[1," + TEST_FEATURES + "]");
    overlayConfig.put("oryx.ml.eval.candidates", 2);
    overlayConfig.put("oryx.ml.eval.parallelism", 2);
    Config config = ConfigUtils.overlayOn(overlayConfig, getConfig());

    startMessaging();

    startServerProduceConsumeTopics(config,
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
    assertNotNull(AppPMMLUtils.getExtensionValue(pmml, "X"));
    assertNotNull(AppPMMLUtils.getExtensionValue(pmml, "Y"));
    assertTrue(Boolean.parseBoolean(AppPMMLUtils.getExtensionValue(pmml, "implicit")));
    assertEquals(0.001, Double.parseDouble(AppPMMLUtils.getExtensionValue(pmml, "lambda")));
    assertEquals(1.0, Double.parseDouble(AppPMMLUtils.getExtensionValue(pmml, "alpha")));
    assertEquals(TEST_FEATURES, Integer.parseInt(AppPMMLUtils.getExtensionValue(pmml, "features")));
  }

}
