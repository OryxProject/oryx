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

package com.cloudera.oryx.app.rdf.tree;

import org.junit.Test;

import com.cloudera.oryx.app.rdf.decision.NumericDecision;
import com.cloudera.oryx.app.rdf.example.Example;
import com.cloudera.oryx.app.rdf.example.NumericFeature;
import com.cloudera.oryx.app.rdf.predict.NumericPrediction;
import com.cloudera.oryx.common.OryxTest;

public final class DecisionTreeTest extends OryxTest {

  static DecisionTree buildTestTree() {
    TerminalNode rnn = new TerminalNode("r--", new NumericPrediction(0.0, 1));
    TerminalNode rnp = new TerminalNode("r-+", new NumericPrediction(1.0, 1));
    DecisionNode rn = new DecisionNode("r-", new NumericDecision(0, -1.0, false), rnn, rnp);
    TerminalNode rp = new TerminalNode("r+", new NumericPrediction(2.0, 1));
    DecisionNode root = new DecisionNode("r", new NumericDecision(0, 1.0, false), rn, rp);
    return new DecisionTree(root);
  }

  @Test
  public void testPredict() {
    DecisionTree tree = buildTestTree();
    NumericPrediction prediction = (NumericPrediction)
        tree.predict(new Example(null, NumericFeature.forValue(0.5)));
    assertEquals(1.0, prediction.getPrediction());
  }

  @Test
  public void testFindTerminal() {
    DecisionTree tree = buildTestTree();
    TerminalNode node = tree.findTerminal(new Example(null, NumericFeature.forValue(0.5)));
    NumericPrediction prediction = (NumericPrediction) node.getPrediction();
    assertEquals(1.0, prediction.getPrediction());
  }

  @Test
  public void testFindByID() {
    DecisionTree tree = buildTestTree();
    TerminalNode node = (TerminalNode) tree.findByID("r-+");
    assertEquals(1.0, ((NumericPrediction) node.getPrediction()).getPrediction());
  }

  @Test
  public void testToString() {
    String s = buildTestTree().toString();
    assertTrue(s.startsWith("(#0 >= 1.0)"));
    assertTrue(s.contains("(#0 >= -1.0)"));
  }

}
