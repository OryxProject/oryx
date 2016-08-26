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

package com.cloudera.oryx.app.batch.mllib.kmeans;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;
import org.dmg.pmml.Model;
import org.dmg.pmml.PMML;
import org.dmg.pmml.clustering.ClusteringModel;
import org.junit.Test;

import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.ml.MLUpdate;

public final class KMeansHyperParamTuningIT extends AbstractKMeansIT {

  private static final int DATA_TO_WRITE = 10000;
  private static final int WRITE_INTERVAL_MSEC = 2;

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
    overlayConfig.put("oryx.kmeans.hyperparams.k", "[2,100]");
    overlayConfig.put("oryx.kmeans.iterations", 20);
    overlayConfig.put("oryx.kmeans.runs", 20);
    overlayConfig.put("oryx.input-schema.num-features", NUM_FEATURES);
    overlayConfig.put("oryx.input-schema.categorical-features", "[]");
    overlayConfig.put("oryx.ml.eval.candidates", 3);
    overlayConfig.put("oryx.ml.eval.parallelism", 2);
    overlayConfig.put("oryx.kmeans.evaluation-strategy", EVALUATION_STRATEGY);

    Config config = ConfigUtils.overlayOn(overlayConfig, getConfig());

    startMessaging();

    startServerProduceConsumeTopics(
        config,
        new RandomKMeansDataGenerator(NUM_FEATURES),
        DATA_TO_WRITE,
        WRITE_INTERVAL_MSEC);

    List<Path> modelInstanceDirs = IOUtils.listFiles(modelDir, "*");

    checkIntervals(modelInstanceDirs.size(), DATA_TO_WRITE, WRITE_INTERVAL_MSEC, GEN_INTERVAL_SEC);

    Path latestModelDir = modelInstanceDirs.get(modelInstanceDirs.size() - 1);
    Path modelFile = latestModelDir.resolve(MLUpdate.MODEL_FILE_NAME);
    assertTrue("No such model file: " + modelFile, Files.exists(modelFile));

    PMML pmml = PMMLUtils.read(modelFile);
    Model rootModel = pmml.getModels().get(0);
    ClusteringModel clusteringModel = (ClusteringModel) rootModel;

    // Should have picked highest k
    assertEquals(100, clusteringModel.getNumberOfClusters());
  }

}
