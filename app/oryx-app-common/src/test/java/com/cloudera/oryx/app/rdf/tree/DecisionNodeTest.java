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

import com.cloudera.oryx.app.rdf.decision.Decision;
import com.cloudera.oryx.app.rdf.decision.NumericDecision;
import com.cloudera.oryx.app.rdf.predict.NumericPrediction;
import com.cloudera.oryx.app.rdf.predict.Prediction;
import com.cloudera.oryx.common.OryxTest;

public final class DecisionNodeTest extends OryxTest {

  @Test
  public void testNode() {
    Decision decision = new NumericDecision(0, -1.5, false);
    TreeNode left = new TerminalNode("2", null);
    TreeNode right = new TerminalNode("3", null);
    DecisionNode node = new DecisionNode("1", decision, left, right);
    assertFalse(node.isTerminal());
    assertSame(decision, node.getDecision());
    assertSame(left, node.getLeft());
    assertSame(right, node.getRight());
  }

  @Test
  public void testEquals() {
    Decision a = new NumericDecision(1, 2.0, true);
    Decision b = new NumericDecision(1, 2.0, true);
    Prediction p = new NumericPrediction(-1.0, 1);
    TreeNode left = new TerminalNode("2", p);
    TreeNode right = new TerminalNode("3", p);
    DecisionNode da = new DecisionNode("a", a, left, right);
    DecisionNode db = new DecisionNode("b", b, left, right);
    assertEquals(da, db);
    assertEquals(da.hashCode(), db.hashCode());
  }

}
