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

package com.cloudera.oryx.app.rdf.predict;

import org.junit.Test;

import com.cloudera.oryx.app.rdf.example.CategoricalFeature;
import com.cloudera.oryx.app.rdf.example.Example;
import com.cloudera.oryx.app.rdf.example.FeatureType;
import com.cloudera.oryx.common.OryxTest;

/**
 * Tests {@link CategoricalPrediction}.
 */
public final class CategoricalPredictionTest extends OryxTest {

  @Test
  public void testConstruct() {
    int[] counts = { 0, 1, 3, 0, 4, 0 };
    CategoricalPrediction prediction = new CategoricalPrediction(counts);
    assertEquals(FeatureType.CATEGORICAL, prediction.getFeatureType());
    assertEquals(4, prediction.getMostProbableCategoryEncoding());
    assertArrayEquals(counts, prediction.getCategoryCounts());
    assertArrayEquals(new double[] {0.0, 0.125, 0.375, 0.0, 0.5, 0.0},
                      prediction.getCategoryProbabilities());
  }

  @Test
  public void testConstructFromProbability() {
    double[] probability = {0.0, 0.125, 0.375, 0.0, 0.5, 0.0 };
    CategoricalPrediction prediction = new CategoricalPrediction(probability);
    assertEquals(FeatureType.CATEGORICAL, prediction.getFeatureType());
    assertEquals(4, prediction.getMostProbableCategoryEncoding());
    assertNull(prediction.getCategoryCounts());
    assertArrayEquals(probability, prediction.getCategoryProbabilities());
  }

  @Test
  public void testUpdate() {
    int[] counts = { 0, 1, 3, 0, 4, 0 };
    CategoricalPrediction prediction = new CategoricalPrediction(counts);
    Example example = new Example(CategoricalFeature.forEncoding(2));
    // Yes, called twice
    prediction.update(example);
    prediction.update(example);
    assertEquals(2, prediction.getMostProbableCategoryEncoding());
    counts[2] += 2;
    assertArrayEquals(counts, prediction.getCategoryCounts());
    assertArrayEquals(new double[] {0.0, 0.1, 0.5, 0.0, 0.4, 0.0},
                      prediction.getCategoryProbabilities());
  }

  @Test
  public void testUpdate2() {
    int[] counts = { 0, 1, 3, 0, 4, 0 };
    CategoricalPrediction prediction = new CategoricalPrediction(counts);
    prediction.update(0, 3);
    prediction.update(1, 9);
    assertArrayEquals(new int[] { 3, 10, 3, 0, 4, 0 }, prediction.getCategoryCounts());
    assertArrayEquals(new double[] {0.15, 0.5, 0.15, 0.0, 0.2, 0.0},
                      prediction.getCategoryProbabilities());
  }

}
