/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
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

package com.cloudera.oryx.app.batch.mllib.rdf;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;
import org.dmg.pmml.PMML;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.app.classreg.example.CategoricalFeature;
import com.cloudera.oryx.app.classreg.example.Example;
import com.cloudera.oryx.app.classreg.predict.NumericPrediction;
import com.cloudera.oryx.app.rdf.RDFPMMLUtils;
import com.cloudera.oryx.app.rdf.tree.DecisionForest;
import com.cloudera.oryx.app.schema.CategoricalValueEncodings;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.ml.MLUpdate;

public final class RDFNumericHyperParamTuningIT extends AbstractRDFIT {

  private static final Logger log = LoggerFactory.getLogger(RDFNumericHyperParamTuningIT.class);

  private static final int DATA_TO_WRITE = 1000;
  private static final int WRITE_INTERVAL_MSEC = 10;
  private static final String IMPURITY = "variance";

  @Test
  public void testRDF() throws Exception {
    Path tempDir = getTempDir();
    Path dataDir = tempDir.resolve("data");
    Path modelDir = tempDir.resolve("model");

    Map<String,Object> overlayConfig = new HashMap<>();
    overlayConfig.put("oryx.batch.update-class", RDFUpdate.class.getName());
    ConfigUtils.set(overlayConfig, "oryx.batch.storage.data-dir", dataDir);
    ConfigUtils.set(overlayConfig, "oryx.batch.storage.model-dir", modelDir);
    overlayConfig.put("oryx.batch.streaming.generation-interval-sec", 15);
    overlayConfig.put("oryx.rdf.num-trees", 1);
    // Low values like 1 are deliberately bad, won't work
    overlayConfig.put("oryx.rdf.hyperparams.max-depth", "[1," + MAX_DEPTH + "]");
    overlayConfig.put("oryx.rdf.hyperparams.max-split-candidates", MAX_SPLIT_CANDIDATES);
    overlayConfig.put("oryx.rdf.hyperparams.impurity", IMPURITY);
    overlayConfig.put("oryx.input-schema.num-features", 5);
    overlayConfig.put("oryx.input-schema.numeric-features", "[\"4\"]");
    overlayConfig.put("oryx.input-schema.id-features", "[\"0\"]");
    overlayConfig.put("oryx.input-schema.target-feature", "\"4\"");
    overlayConfig.put("oryx.ml.eval.candidates", 2);
    overlayConfig.put("oryx.ml.eval.parallelism", 2);
    Config config = ConfigUtils.overlayOn(overlayConfig, getConfig());

    startMessaging();

    startServerProduceConsumeTopics(
        config,
        new RandomNumericRDFDataGenerator(3),
        DATA_TO_WRITE,
        WRITE_INTERVAL_MSEC);

    List<Path> modelInstanceDirs = IOUtils.listFiles(modelDir, "*");

    checkIntervals(modelInstanceDirs.size(), DATA_TO_WRITE, WRITE_INTERVAL_MSEC, GEN_INTERVAL_SEC);

    Path latestModelDir = modelInstanceDirs.get(modelInstanceDirs.size() - 1);
    Path modelFile = latestModelDir.resolve(MLUpdate.MODEL_FILE_NAME);
    assertTrue("No such model file: " + modelFile, Files.exists(modelFile));

    PMML pmml = PMMLUtils.read(modelFile);

    assertEquals(3, pmml.getExtensions().size());
    Map<String,Object> expected = new HashMap<>();
    expected.put("maxSplitCandidates", MAX_SPLIT_CANDIDATES);
    expected.put("maxDepth", MAX_DEPTH);
    expected.put("impurity", IMPURITY);
    checkExtensions(pmml, expected);

    Pair<DecisionForest,CategoricalValueEncodings> forestEncoding = RDFPMMLUtils.read(pmml);
    DecisionForest forest = forestEncoding.getFirst();
    log.info("\n{}", forest);
    CategoricalValueEncodings encoding = forestEncoding.getSecond();

    for (int f1 = 0; f1 <= 1; f1++) {
      CategoricalFeature feature1 = CategoricalFeature.forEncoding(
          encoding.getValueEncodingMap(1).get(f1 == 1 ? "A" : "B"));
      for (int f2 = 0; f2 <= 1; f2++) {
        CategoricalFeature feature2 = CategoricalFeature.forEncoding(
            encoding.getValueEncodingMap(2).get(f2 == 1 ? "A" : "B"));
        for (int f3 = 0; f3 <= 1; f3++) {
          CategoricalFeature feature3 = CategoricalFeature.forEncoding(
              encoding.getValueEncodingMap(3).get(f3 == 1 ? "A" : "B"));
          Example toPredict = new Example(null, null, feature1, feature2, feature3);
          double prediction = ((NumericPrediction) forest.predict(toPredict)).getPrediction();
          assertEquals("Incorrect prediction " + prediction + " for " + toPredict,
                       f1 + f2 + f3, Math.round(prediction));
        }
      }
    }

  }

}
