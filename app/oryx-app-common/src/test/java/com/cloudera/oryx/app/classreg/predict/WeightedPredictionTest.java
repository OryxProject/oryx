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

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.cloudera.oryx.app.classreg.example.FeatureType;
import com.cloudera.oryx.common.OryxTest;

/**
 * Tests {@link WeightedPrediction}.
 */
public final class WeightedPredictionTest extends OryxTest {

  @Test
  public void testNumericVote() {
    List<NumericPrediction> predictions = Arrays.asList(
        new NumericPrediction(1.0, 1),
        new NumericPrediction(3.0, 2),
        new NumericPrediction(6.0, 3)
    );
    double[] weights = {1.0, 1.0, 1.0};
    NumericPrediction vote =
        (NumericPrediction) WeightedPrediction.voteOnFeature(predictions, weights);
    assertEquals(FeatureType.NUMERIC, vote.getFeatureType());
    assertEquals(10.0 /3.0, vote.getPrediction());
  }

  @Test
  public void testNumericVoteWeighted() {
    List<NumericPrediction> predictions = Arrays.asList(
        new NumericPrediction(1.0, 1),
        new NumericPrediction(3.0, 2),
        new NumericPrediction(6.0, 3)
    );
    double[] weights = {3.0, 2.0, 1.0};
    NumericPrediction vote =
        (NumericPrediction) WeightedPrediction.voteOnFeature(predictions, weights);
    assertEquals(FeatureType.NUMERIC, vote.getFeatureType());
    assertEquals(15.0 / 6.0, vote.getPrediction());
  }

  @Test
  public void testCategoricalVote() {
    List<CategoricalPrediction> predictions = Arrays.asList(
        new CategoricalPrediction(new int[]{0, 1, 2}),
        new CategoricalPrediction(new int[]{6, 2, 0}),
        new CategoricalPrediction(new int[]{0, 2, 0})
    );
    double[] weights = {1.0, 1.0, 1.0};
    CategoricalPrediction vote =
        (CategoricalPrediction) WeightedPrediction.voteOnFeature(predictions, weights);
    assertEquals(FeatureType.CATEGORICAL, vote.getFeatureType());
    assertEquals(1, vote.getMostProbableCategoryEncoding());
  }

  @Test
  public void testCategoricalVoteWeighted() {
    List<CategoricalPrediction> predictions = Arrays.asList(
        new CategoricalPrediction(new int[]{0, 1, 2}),
        new CategoricalPrediction(new int[]{6, 2, 0}),
        new CategoricalPrediction(new int[]{0, 2, 0})
    );
    double[] weights = {1.0, 10.0, 1.0};
    CategoricalPrediction vote =
        (CategoricalPrediction) WeightedPrediction.voteOnFeature(predictions, weights);
    assertEquals(FeatureType.CATEGORICAL, vote.getFeatureType());
    assertEquals(0, vote.getMostProbableCategoryEncoding());
  }

}
