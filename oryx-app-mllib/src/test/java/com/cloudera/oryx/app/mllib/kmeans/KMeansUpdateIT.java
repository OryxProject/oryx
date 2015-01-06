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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;
import org.dmg.pmml.ClusteringModel;
import org.dmg.pmml.ComparisonMeasure;
import org.dmg.pmml.Model;
import org.dmg.pmml.PMML;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.ml.MLUpdate;

public final class KMeansUpdateIT extends AbstractKMeansIT {

  private static final Logger log = LoggerFactory.getLogger(KMeansUpdateIT.class);

  private static final int DATA_TO_WRITE = 2000;
  private static final int WRITE_INTERVAL_MSEC = 10;
  private static final int CLUSTERS = 3;

  @Ignore
  @Test
  public void testKMeans() throws Exception {
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
    overlayConfig.put("oryx.input-schema.num-features", 2);
    overlayConfig.put("oryx.input-schema.numeric-features", "[\"0\",\"1\"]");
    overlayConfig.put("oryx.kmeans.iterations", 5);

    Config config = ConfigUtils.overlayOn(overlayConfig, getConfig());

    startMessaging();

    List<Pair<String, String>> updates = startServerProduceConsumeTopics(
        config,
        new RandomKMeansDataGenerator(2),
        DATA_TO_WRITE,
        WRITE_INTERVAL_MSEC);

    List<Path> modelInstanceDirs = IOUtils.listFiles(modelDir, "*");
    log.info("Model instance dirs: {}", modelInstanceDirs);
    assertFalse("No models?", modelInstanceDirs.isEmpty());

    int generations = modelInstanceDirs.size();
    checkIntervals(generations, DATA_TO_WRITE, WRITE_INTERVAL_MSEC, GEN_INTERVAL_SEC);

    for (Path modelInstanceDir : modelInstanceDirs) {
      log.info("Testing model instance dir {}", modelInstanceDir);
      Path modelFile = modelInstanceDir.resolve(MLUpdate.MODEL_FILE_NAME);
      assertTrue("Model file should exist: " + modelFile, Files.exists(modelFile));
      assertTrue("Model file should not be empty: " + modelFile, Files.size(modelFile) > 0);
      PMMLUtils.read(modelFile); // Shouldn't throw exception
    }

    for (Pair<String,String> km : updates) {

      String type = km.getFirst();
      String value = km.getSecond();

      assertEquals("MODEL", type);
      log.info("{}", value);

      PMML pmml = PMMLUtils.fromString(value);

      checkHeader(pmml.getHeader());

      Model rootModel = pmml.getModels().get(0);
      assertTrue(rootModel instanceof ClusteringModel);

      ClusteringModel clusteringModel = ((ClusteringModel) rootModel);

      // Check if Basic hyperparameters match
      assertEquals(Integer.valueOf(CLUSTERS), clusteringModel.getNumberOfClusters());
      assertEquals(ComparisonMeasure.Kind.DISTANCE, clusteringModel.getComparisonMeasure().getKind());

      assertEquals(2, clusteringModel.getClusters().get(0).getArray().getN().intValue());

    }
  }

}
