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

package com.cloudera.oryx.app.classreg.predict;

import org.junit.Test;

import com.cloudera.oryx.app.classreg.example.Example;
import com.cloudera.oryx.app.classreg.example.FeatureType;
import com.cloudera.oryx.app.classreg.example.NumericFeature;
import com.cloudera.oryx.common.OryxTest;

/**
 * Tests {@link NumericPrediction}.
 */
public final class NumericPredictionTest extends OryxTest {

  @Test
  public void testConstruct() {
    NumericPrediction prediction = new NumericPrediction(1.5, 1);
    assertEquals(FeatureType.NUMERIC, prediction.getFeatureType());
    assertEquals(1.5, prediction.getPrediction());
  }

  @Test
  public void testUpdate() {
    NumericPrediction prediction = new NumericPrediction(1.5, 1);
    Example example = new Example(NumericFeature.forValue(2.5));
    prediction.update(example);
    assertEquals(2.0, prediction.getPrediction());
  }

  @Test
  public void testUpdate2() {
    NumericPrediction prediction = new NumericPrediction(1.5, 1);
    prediction.update(3.5, 3);
    assertEquals(3.0, prediction.getPrediction());
  }

  @Test
  public void testHashCode() {
    NumericPrediction prediction = new NumericPrediction(1.5, 1);
    assertEquals(1073217536, prediction.hashCode());
    prediction.update(2.0, 2);
    assertEquals(1789394944, prediction.hashCode());
  }

  @Test
  public void testEquals() {
    NumericPrediction prediction = new NumericPrediction(1.5, 1);
    NumericPrediction prediction1 = new NumericPrediction(1.5, 2);
    assertEquals(prediction, prediction1);
    prediction1.update(2.0, 2);
    assertNotEquals(prediction, prediction1);
    prediction1.update(1.5, 4);
    assertNotEquals(prediction, prediction1);
  }

}
