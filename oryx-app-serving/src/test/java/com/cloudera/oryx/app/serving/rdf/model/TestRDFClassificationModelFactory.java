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

import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.typesafe.config.Config;

import com.cloudera.oryx.app.rdf.decision.CategoricalDecision;
import com.cloudera.oryx.app.rdf.decision.Decision;
import com.cloudera.oryx.app.rdf.decision.NumericDecision;
import com.cloudera.oryx.app.rdf.predict.CategoricalPrediction;
import com.cloudera.oryx.app.rdf.tree.DecisionForest;
import com.cloudera.oryx.app.rdf.tree.DecisionNode;
import com.cloudera.oryx.app.rdf.tree.DecisionTree;
import com.cloudera.oryx.app.rdf.tree.TerminalNode;
import com.cloudera.oryx.app.rdf.tree.TreeNode;
import com.cloudera.oryx.app.schema.CategoricalValueEncodings;
import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.common.settings.ConfigUtils;

public final class TestRDFClassificationModelFactory {

  private TestRDFClassificationModelFactory() {}

  public static RDFServingModel buildTestModel() {
    Map<Integer,Collection<String>> distinctValues = new HashMap<>();
    distinctValues.put(0, Arrays.asList("A", "B", "C"));
    distinctValues.put(2, Arrays.asList("X", "Y", "Z"));
    CategoricalValueEncodings encodings = new CategoricalValueEncodings(distinctValues);

    TerminalNode left1 = new TerminalNode("r-", new CategoricalPrediction(new int[] { 1, 2, 3 }));
    TerminalNode right1 = new TerminalNode("r+", new CategoricalPrediction(new int[] { 10, 30, 50 }));
    BitSet activeCategories = new BitSet(2);
    activeCategories.set(1);
    Decision decision1 = new CategoricalDecision(0, activeCategories, true);
    TreeNode root1 = new DecisionNode("r", decision1, left1, right1);

    TerminalNode left2 = new TerminalNode("r-", new CategoricalPrediction(new int[] { 100, 400, 900 }));
    TerminalNode right2 = new TerminalNode("r+", new CategoricalPrediction(new int[] { 1000, 10000, 100000 }));
    Decision decision2 = new NumericDecision(1, -3.0, false);
    TreeNode root2 = new DecisionNode("r", decision2, left2, right2);

    DecisionTree tree1 = new DecisionTree(root1);
    DecisionTree tree2 = new DecisionTree(root2);
    DecisionTree[] trees = { tree1, tree2 };
    double[] weights = { 1.0, 2.0 };
    double[] featureImportances = { 0.1, 0.3 };
    DecisionForest forest = new DecisionForest(trees, weights, featureImportances);

    Map<String,Object> overlayConfig = new HashMap<>();
    overlayConfig.put("oryx.input-schema.num-features", 3);
    overlayConfig.put("oryx.input-schema.categorical-features", "[\"0\",\"2\"]");
    overlayConfig.put("oryx.input-schema.target-feature", "\"2\"");
    Config config = ConfigUtils.overlayOn(overlayConfig, ConfigUtils.getDefault());
    InputSchema inputSchema = new InputSchema(config);

    return new RDFServingModel(forest, encodings, inputSchema);
  }

}
