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

import java.util.BitSet;

import org.junit.Test;

import com.cloudera.oryx.app.classreg.example.CategoricalFeature;
import com.cloudera.oryx.app.classreg.example.Example;
import com.cloudera.oryx.app.classreg.example.Feature;
import com.cloudera.oryx.app.classreg.example.FeatureType;
import com.cloudera.oryx.common.OryxTest;

/**
 * Tests {@link CategoricalDecision}.
 */
public final class CategoricalDecisionTest extends OryxTest {

  @Test
  public void testDecisionBasics() {
    CategoricalDecision decision = new CategoricalDecision(0, new BitSet(1), false);
    assertEquals(0, decision.getFeatureNumber());
    assertEquals(new BitSet(1), decision.getActiveCategoryEncodings());
    assertFalse(decision.getDefaultDecision());
    assertEquals(FeatureType.CATEGORICAL, decision.getType());
  }

  @Test
  public void testDecision() {
    BitSet activeCategories = new BitSet(10);
    activeCategories.set(2);
    activeCategories.set(5);
    Decision decision = new CategoricalDecision(0, activeCategories, true);
    for (int i = 0; i < 10; i++) {
      assertEquals(activeCategories.get(i),
                   decision.isPositive(new Example(null, CategoricalFeature.forEncoding(i))));
    }
    assertTrue(decision.isPositive(new Example(null, new Feature[] {null})));
  }

  @Test
  public void testToString() {
    BitSet activeCategories = new BitSet(10);
    activeCategories.set(2);
    activeCategories.set(5);
    CategoricalDecision decision = new CategoricalDecision(0, activeCategories, true);
    assertEquals("(#0 ∈ [2,5])", decision.toString());
  }

  @Test
  public void testEqualsHashCode() {
    BitSet activeCategories = new BitSet(10);
    activeCategories.set(5);
    CategoricalDecision a = new CategoricalDecision(0, activeCategories, true);
    CategoricalDecision b = new CategoricalDecision(0, activeCategories, true);
    CategoricalDecision c = new CategoricalDecision(1, activeCategories, true);
    assertEquals(a, b);
    assertNotEquals(a, c);
    assertEquals(a.hashCode(), b.hashCode());
    assertNotEquals(a.hashCode(), c.hashCode());
  }

}
