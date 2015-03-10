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

import com.cloudera.oryx.app.rdf.predict.NumericPrediction;
import com.cloudera.oryx.app.rdf.predict.Prediction;
import com.cloudera.oryx.common.OryxTest;

public final class TerminalNodeTest extends OryxTest {

  @Test
  public void testNode() {
    Prediction prediction = new NumericPrediction(1.2, 3);
    TerminalNode node = new TerminalNode("1", prediction);
    assertTrue(node.isTerminal());
    assertSame(prediction, node.getPrediction());
    assertEquals(3, node.getCount());
  }

}
