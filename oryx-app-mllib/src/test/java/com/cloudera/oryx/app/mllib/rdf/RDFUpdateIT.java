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

package com.cloudera.oryx.app.mllib.rdf;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;
import org.dmg.pmml.Extension;
import org.dmg.pmml.MiningModel;
import org.dmg.pmml.Model;
import org.dmg.pmml.PMML;
import org.dmg.pmml.TreeModel;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.app.pmml.AppPMMLUtils;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.ml.MLUpdate;

public final class RDFUpdateIT extends AbstractRDFIT {

  private static final Logger log = LoggerFactory.getLogger(RDFUpdateIT.class);

  private static final int DATA_TO_WRITE = 2000;
  private static final int WRITE_INTERVAL_MSEC = 10;
  private static final int GEN_INTERVAL_SEC = 10;
  private static final int BLOCK_INTERVAL_SEC = 1;
  private static final int NUM_TREES = 2;
  private static final int MAX_DEPTH = 8;
  private static final int MAX_SPLIT_CANDIDATES = 100;
  private static final String IMPURITY = "entropy";

  // TODO unignore!
  @Ignore
  @Test
  public void testRDF() throws Exception {
    Path tempDir = getTempDir();
    Path dataDir = tempDir.resolve("data");
    Path modelDir = tempDir.resolve("model");

    Map<String,Object> overlayConfig = new HashMap<>();
    overlayConfig.put("oryx.batch.update-class", RDFUpdate.class.getName());
    ConfigUtils.set(overlayConfig, "oryx.batch.storage.data-dir", dataDir);
    ConfigUtils.set(overlayConfig, "oryx.batch.storage.model-dir", modelDir);
    overlayConfig.put("oryx.batch.streaming.generation-interval-sec", GEN_INTERVAL_SEC);
    overlayConfig.put("oryx.batch.streaming.block-interval-sec", BLOCK_INTERVAL_SEC);
    overlayConfig.put("oryx.rdf.num-trees", NUM_TREES);
    overlayConfig.put("oryx.rdf.hyperparams.max-depth", MAX_DEPTH);
    overlayConfig.put("oryx.rdf.hyperparams.max-split-candidates", MAX_SPLIT_CANDIDATES);
    overlayConfig.put("oryx.rdf.hyperparams.impurity", IMPURITY);
    overlayConfig.put("oryx.input-schema.num-features", 5);
    overlayConfig.put("oryx.input-schema.categorical-features", "[\"4\"]");
    overlayConfig.put("oryx.input-schema.id-features", "[\"0\"]");
    overlayConfig.put("oryx.input-schema.target-feature", "\"4\"");

    Config config = ConfigUtils.overlayOn(overlayConfig, getConfig());

    startMessaging();

    List<Pair<String, String>> updates = startServerProduceConsumeTopics(
        config,
        new RandomRDFDataGenerator(3),
        DATA_TO_WRITE,
        WRITE_INTERVAL_MSEC);

    List<Path> modelInstanceDirs = IOUtils.listFiles(modelDir, "*");
    log.info("Model instance dirs: {}", modelInstanceDirs);

    int generations = modelInstanceDirs.size();
    checkIntervals(generations, DATA_TO_WRITE, WRITE_INTERVAL_MSEC, GEN_INTERVAL_SEC);

    for (Path modelInstanceDir : modelInstanceDirs) {
      log.info("Testing model instance dir {}", modelInstanceDir);
      Path modelFile = modelInstanceDir.resolve(MLUpdate.MODEL_FILE_NAME);
      assertTrue("Model file should exist: " + modelFile, Files.exists(modelFile));
      assertTrue("Model file should not be empty: " + modelFile, Files.size(modelFile) > 0);
    }

    for (Pair<String,String> km : updates) {

      String type = km.getFirst();
      String value = km.getSecond();

      log.debug("{} = {}", type, value);

      assertEquals("MODEL", type);

      PMML pmml = PMMLUtils.fromString(value);
      List<Extension> extensions = pmml.getExtensions();
      assertEquals(3, extensions.size());
      // Basic hyperparameters should match
      assertEquals(Integer.toString(MAX_DEPTH), AppPMMLUtils.getExtensionValue(pmml, "maxDepth"));
      assertEquals(Integer.toString(MAX_SPLIT_CANDIDATES),
                   AppPMMLUtils.getExtensionValue(pmml, "maxSplitCandidates"));
      assertEquals(IMPURITY, AppPMMLUtils.getExtensionValue(pmml, "impurity"));

      Model rootModel = pmml.getModels().get(0);
      int actualNumTrees;
      if (rootModel instanceof TreeModel) {
        actualNumTrees = 1;
      } else if (rootModel instanceof MiningModel) {
        actualNumTrees = ((MiningModel) rootModel).getSegmentation().getSegments().size();
      } else {
        fail("Wrong model type");
        return;
      }
      assertEquals(NUM_TREES, actualNumTrees);

    }
  }

}
