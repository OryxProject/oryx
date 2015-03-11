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
import org.dmg.pmml.Cluster;
import org.dmg.pmml.ClusteringModel;
import org.dmg.pmml.ComparisonMeasure;
import org.dmg.pmml.Model;
import org.dmg.pmml.PMML;
import org.junit.Test;

import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.ml.batch.MLUpdate;

public final class KMeansUpdateIT extends AbstractKMeansIT {

  private static final int DATA_TO_WRITE = 2000;
  private static final int WRITE_INTERVAL_MSEC = 10;

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
    overlayConfig.put("oryx.kmeans.hyperparams.k", NUM_CLUSTERS);
    overlayConfig.put("oryx.kmeans.iterations", 5);
    overlayConfig.put("oryx.input-schema.num-features", NUM_FEATURES);
    overlayConfig.put("oryx.input-schema.categorical-features", "[]");
    overlayConfig.put("oryx.kmeans.evaluation-strategy", EVALUATION_STRATEGY);

    Config config = ConfigUtils.overlayOn(overlayConfig, getConfig());

    startMessaging();

    List<Pair<String, String>> updates = startServerProduceConsumeTopics(
        config,
        new RandomKMeansDataGenerator(NUM_FEATURES),
        DATA_TO_WRITE,
        WRITE_INTERVAL_MSEC);

    List<Path> modelInstanceDirs = IOUtils.listFiles(modelDir, "*");

    int generations = modelInstanceDirs.size();
    checkIntervals(generations, DATA_TO_WRITE, WRITE_INTERVAL_MSEC, GEN_INTERVAL_SEC);

    for (Path modelInstanceDir : modelInstanceDirs) {
      Path modelFile = modelInstanceDir.resolve(MLUpdate.MODEL_FILE_NAME);
      assertTrue("Model file should exist: " + modelFile, Files.exists(modelFile));
      assertTrue("Model file should not be empty: " + modelFile, Files.size(modelFile) > 0);
      PMMLUtils.read(modelFile); // Shouldn't throw exception
    }

    InputSchema schema = new InputSchema(config);

    for (Pair<String,String> km : updates) {

      String type = km.getFirst();
      String value = km.getSecond();

      assertEquals("MODEL", type);

      PMML pmml = PMMLUtils.fromString(value);

      checkHeader(pmml.getHeader());

      checkDataDictionary(schema, pmml.getDataDictionary());

      Model rootModel = pmml.getModels().get(0);

      ClusteringModel clusteringModel = (ClusteringModel) rootModel;

      // Check if Basic hyperparameters match
      assertEquals(NUM_CLUSTERS, clusteringModel.getNumberOfClusters().intValue());
      assertEquals(NUM_CLUSTERS, clusteringModel.getClusters().size());
      assertEquals(NUM_FEATURES, clusteringModel.getClusteringFields().size());
      assertEquals(ComparisonMeasure.Kind.DISTANCE,
                   clusteringModel.getComparisonMeasure().getKind());
      assertEquals(NUM_FEATURES, clusteringModel.getClusters().get(0).getArray().getN().intValue());
      for (Cluster cluster : clusteringModel.getClusters()) {
        assertTrue(cluster.getSize() > 0);
      }
    }
  }

}
