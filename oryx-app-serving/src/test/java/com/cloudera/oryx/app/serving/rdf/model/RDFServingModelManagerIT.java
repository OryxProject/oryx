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

package com.cloudera.oryx.app.serving.rdf.model;

import java.util.HashMap;
import java.util.Map;

import com.typesafe.config.Config;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.app.rdf.predict.CategoricalPrediction;
import com.cloudera.oryx.app.rdf.tree.DecisionForest;
import com.cloudera.oryx.app.rdf.tree.DecisionNode;
import com.cloudera.oryx.app.rdf.tree.DecisionTree;
import com.cloudera.oryx.app.rdf.tree.TerminalNode;
import com.cloudera.oryx.app.schema.CategoricalValueEncodings;
import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.app.serving.AbstractOryxResource;
import com.cloudera.oryx.app.speed.rdf.MockRDFClassificationModelGenerator;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.lambda.serving.AbstractServingIT;

public final class RDFServingModelManagerIT extends AbstractServingIT {

  private static final Logger log = LoggerFactory.getLogger(RDFServingModelManagerIT.class);

  @Test
  public void testRDFServingModel() throws Exception {
    Map<String,Object> overlayConfig = new HashMap<>();
    overlayConfig.put("oryx.serving.application-resources",
                      "\"com.cloudera.oryx.app.serving,com.cloudera.oryx.app.serving.rdf\"");
    overlayConfig.put("oryx.serving.model-manager-class", RDFServingModelManager.class.getName());
    overlayConfig.put("oryx.input-schema.feature-names", "[\"color\",\"fruit\"]");
    overlayConfig.put("oryx.input-schema.numeric-features", "[]");
    overlayConfig.put("oryx.input-schema.target-feature", "fruit");
    Config config = ConfigUtils.overlayOn(overlayConfig, getConfig());

    startMessaging();
    startServer(config);
    // Creates 1 model, 2 updates for each of r- and r+
    startUpdateTopics(new MockRDFClassificationModelGenerator(), 5);

    // Let updates finish
    Thread.sleep(1000);

    RDFServingModelManager manager = (RDFServingModelManager)
        getServingLayer().getContext().getServletContext().getAttribute(
            AbstractOryxResource.MODEL_MANAGER_KEY);

    assertNotNull("Manager must initialize in web context", manager);

    RDFServingModel model = manager.getModel();
    log.debug("{}", model);

    CategoricalValueEncodings encodings = model.getEncodings();
    assertEquals(2, encodings.getValueCount(0));
    assertEquals(2, encodings.getValueCount(1));
    Map<Integer,String> encodingValuePredictor = encodings.getEncodingValueMap(0);
    assertEquals("yellow", encodingValuePredictor.get(0));
    assertEquals("red", encodingValuePredictor.get(1));
    Map<Integer,String> encodingValueTarget = encodings.getEncodingValueMap(1);
    assertEquals("banana", encodingValueTarget.get(0));
    assertEquals("apple", encodingValueTarget.get(1));

    DecisionForest forest = model.getForest();

    DecisionTree[] trees = forest.getTrees();
    assertEquals(1, trees.length);
    assertArrayEquals(new double[] { 1.0 }, forest.getWeights());

    InputSchema inputSchema = model.getInputSchema();
    assertEquals(2, inputSchema.getNumFeatures());

    DecisionTree tree = trees[0];
    DecisionNode root = (DecisionNode) tree.findByID("r");

    TerminalNode left = (TerminalNode) tree.findByID("r-");
    TerminalNode right = (TerminalNode) tree.findByID("r+");
    assertSame(root.getLeft(), left);
    assertSame(root.getRight(), right);
    assertEquals(7, left.getCount());
    assertEquals(7, right.getCount());

    CategoricalPrediction leftPrediction = (CategoricalPrediction) left.getPrediction();
    CategoricalPrediction rightPrediction = (CategoricalPrediction) right.getPrediction();
    assertEquals(2, leftPrediction.getCategoryCounts()[0]);
    assertEquals(5, leftPrediction.getCategoryCounts()[1]);
    assertEquals(3, rightPrediction.getCategoryCounts()[0]);
    assertEquals(4, rightPrediction.getCategoryCounts()[1]);
  }

}
