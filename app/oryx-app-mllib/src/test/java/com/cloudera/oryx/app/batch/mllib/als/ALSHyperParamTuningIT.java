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

package com.cloudera.oryx.app.batch.mllib.als;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;
import org.dmg.pmml.PMML;
import org.junit.Test;

import com.cloudera.oryx.app.batch.mllib.AbstractAppMLlibIT;
import com.cloudera.oryx.app.pmml.AppPMMLUtils;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.lambda.batch.AbstractBatchIT;
import com.cloudera.oryx.ml.MLUpdate;

public final class ALSHyperParamTuningIT extends AbstractALSIT {

  private static final int DATA_TO_WRITE = 10000;
  private static final int WRITE_INTERVAL_MSEC = 2;
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
    overlayConfig.put("oryx.batch.streaming.generation-interval-sec",
                      AbstractAppMLlibIT.GEN_INTERVAL_SEC);
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

    AbstractBatchIT.checkIntervals(modelInstanceDirs.size(),
                                   DATA_TO_WRITE,
                                   WRITE_INTERVAL_MSEC,
                                   AbstractAppMLlibIT.GEN_INTERVAL_SEC);

    Path latestModelDir = modelInstanceDirs.get(modelInstanceDirs.size() - 1);
    Path modelFile = latestModelDir.resolve(MLUpdate.MODEL_FILE_NAME);
    assertTrue("No such model file: " + modelFile, Files.exists(modelFile));

    PMML pmml = PMMLUtils.read(modelFile);
    assertEquals(9, pmml.getExtensions().size());
    assertNotNull(AppPMMLUtils.getExtensionValue(pmml, "X"));
    assertNotNull(AppPMMLUtils.getExtensionValue(pmml, "Y"));
    Map<String,Object> expected = new HashMap<>();
    expected.put("features", TEST_FEATURES);
    expected.put("lambda", 0.001);
    expected.put("implicit", true);
    expected.put("alpha", 1.0);
    expected.put("logStrength", false);
    checkExtensions(pmml, expected);
  }

}
