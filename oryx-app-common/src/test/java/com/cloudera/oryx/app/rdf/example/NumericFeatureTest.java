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

package com.cloudera.oryx.app.rdf.example;

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

/**
 * Tests {@link NumericFeature}.
 */
public final class NumericFeatureTest extends OryxTest {

  @Test
  public void testFeature() {
    NumericFeature f = NumericFeature.forValue(1.5);
    assertEquals(FeatureType.NUMERIC, f.getFeatureType());
    assertEquals(1.5, f.getValue());
    assertEquals(f, NumericFeature.forValue(1.5));
  }

  @Test
  public void testToString() {
    NumericFeature f = NumericFeature.forValue(1.5);
    assertEquals("1.5", f.toString());
  }

  @Test
  public void testEquals() {
    NumericFeature f = NumericFeature.forValue(1.5);
    assertFalse(f.equals(NumericFeature.forValue(Double.NaN)));
    assertTrue(f.equals(NumericFeature.forValue(1.5)));
  }

  @Test
  public void testHashCode() {
    assertEquals(NumericFeature.forValue(1.5), NumericFeature.forValue(1.5));
    assertEquals(NumericFeature.forValue(Double.MIN_VALUE), NumericFeature.forValue(Double.MIN_VALUE));
  }

}
