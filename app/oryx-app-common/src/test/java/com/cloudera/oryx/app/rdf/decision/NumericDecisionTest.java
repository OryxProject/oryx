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

package com.cloudera.oryx.app.rdf.decision;

import org.junit.Test;

import com.cloudera.oryx.app.rdf.example.Example;
import com.cloudera.oryx.app.rdf.example.Feature;
import com.cloudera.oryx.app.rdf.example.FeatureType;
import com.cloudera.oryx.app.rdf.example.NumericFeature;
import com.cloudera.oryx.common.OryxTest;

/**
 * Tests {@link NumericDecision}.
 */
public final class NumericDecisionTest extends OryxTest {

  @Test
  public void testDecisionBasics() {
    NumericDecision decision = new NumericDecision(0, -1.5, false);
    assertEquals(0, decision.getFeatureNumber());
    assertEquals(-1.5, decision.getThreshold());
    assertFalse(decision.getDefaultDecision());
    assertEquals(FeatureType.NUMERIC, decision.getType());
  }

  @Test
  public void testDecision() {
    Decision decision = new NumericDecision(0, -3.1, true);
    assertFalse(decision.isPositive(new Example(null, NumericFeature.forValue(-3.5))));
    assertTrue(decision.isPositive(new Example(null, NumericFeature.forValue(-3.1))));
    assertTrue(decision.isPositive(new Example(null, NumericFeature.forValue(-3.0))));
    assertTrue(decision.isPositive(new Example(null, NumericFeature.forValue(3.1))));
    assertTrue(decision.isPositive(new Example(null, new Feature[] {null})));
  }

}
