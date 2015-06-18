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

package com.cloudera.oryx.app.batch.mllib.rdf;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.dmg.pmml.MiningFunctionType;
import org.dmg.pmml.MiningModel;
import org.dmg.pmml.MissingValueStrategyType;
import org.dmg.pmml.Model;
import org.dmg.pmml.MultipleModelMethodType;
import org.dmg.pmml.Node;
import org.dmg.pmml.PMML;
import org.dmg.pmml.ScoreDistribution;
import org.dmg.pmml.Segment;
import org.dmg.pmml.Segmentation;
import org.dmg.pmml.TreeModel;
import org.dmg.pmml.True;
import org.junit.Test;

import com.cloudera.oryx.app.pmml.AppPMMLUtils;
import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.ml.MLUpdate;

public final class RDFUpdateIT extends AbstractRDFIT {

  private static final int DATA_TO_WRITE = 2000;
  private static final int WRITE_INTERVAL_MSEC = 10;
  private static final int NUM_TREES = 2;

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
        new RandomCategoricalRDFDataGenerator(3),
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

      assertTrue("MODEL".equals(type) || "MODEL-REF".equals(type));
      PMML pmml = AppPMMLUtils.readPMMLFromUpdateKeyMessage(type, value, new Configuration());

      checkHeader(pmml.getHeader());

      assertEquals(3, pmml.getExtensions().size());
      Map<String,Object> expected = new HashMap<>();
      expected.put("maxDepth", MAX_DEPTH);
      expected.put("maxSplitCandidates", MAX_SPLIT_CANDIDATES);
      expected.put("impurity", IMPURITY);
      checkExtensions(pmml, expected);

      checkDataDictionary(schema, pmml.getDataDictionary());

      Model rootModel = pmml.getModels().get(0);
      if (rootModel instanceof TreeModel) {
        assertEquals(NUM_TREES, 1);
        TreeModel treeModel = (TreeModel) rootModel;
        checkTreeModel(treeModel);
      } else if (rootModel instanceof MiningModel) {
        MiningModel miningModel = (MiningModel) rootModel;
        Segmentation segmentation = miningModel.getSegmentation();
        if (schema.isClassification()) {
          assertEquals(MultipleModelMethodType.WEIGHTED_MAJORITY_VOTE,
                       segmentation.getMultipleModelMethod());
        } else {
          assertEquals(MultipleModelMethodType.WEIGHTED_AVERAGE,
                       segmentation.getMultipleModelMethod());
        }
        List<Segment> segments = segmentation.getSegments();
        assertEquals(NUM_TREES, segments.size());
        for (int i = 0; i < segments.size(); i++) {
          Segment segment = segments.get(i);
          assertEquals(Integer.toString(i), segment.getId());
          assertTrue(segment.getPredicate() instanceof True);
          assertEquals(1.0, segment.getWeight());
          assertTrue(segment.getModel() instanceof TreeModel);
          checkTreeModel((TreeModel) segment.getModel());
        }

      } else {
        fail("Wrong model type: " + rootModel.getClass());
        return;
      }

      if (schema.isClassification()) {
        assertEquals(MiningFunctionType.CLASSIFICATION, rootModel.getFunctionName());
      } else {
        assertEquals(MiningFunctionType.REGRESSION, rootModel.getFunctionName());
      }

      checkMiningSchema(schema, rootModel.getMiningSchema());

    }
  }

  private static void checkTreeModel(TreeModel treeModel) {
    assertEquals(TreeModel.SplitCharacteristic.BINARY_SPLIT, treeModel.getSplitCharacteristic());
    assertEquals(MissingValueStrategyType.DEFAULT_CHILD, treeModel.getMissingValueStrategy());
    checkNode(treeModel.getNode());
  }

  private static void checkNode(Node node) {
    assertNotNull(node.getId());
    List<ScoreDistribution> scoreDists = node.getScoreDistributions();
    if (scoreDists.isEmpty()) {
      // Non-leaf
      List<Node> children = node.getNodes();
      assertEquals(2, children.size());
      Node rightChild = children.get(0);
      Node leftChild = children.get(1);
      assertTrue(leftChild.getPredicate() instanceof True);
      assertEquals(node.getRecordCount().doubleValue(),
                   leftChild.getRecordCount() + rightChild.getRecordCount());
      assertEquals(node.getId() + "+", rightChild.getId());
      assertEquals(node.getId() + "-", leftChild.getId());
      checkNode(rightChild);
      checkNode(leftChild);
    } else {
      // Leaf
      assertEquals(1, scoreDists.size());
    }
  }

}
