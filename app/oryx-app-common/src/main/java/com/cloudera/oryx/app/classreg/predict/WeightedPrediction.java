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

import java.util.List;
import java.util.Objects;

import com.google.common.base.Preconditions;

import com.cloudera.oryx.common.math.DoubleWeightedMean;

/**
 * A utility class with methods for combining the results of many
 * {@link com.cloudera.oryx.app.rdf.tree.TreeBasedClassifier}s into one prediction -- in particular, it is used by
 * {@link com.cloudera.oryx.app.rdf.tree.DecisionForest} to combine the  results of many
 * {@link com.cloudera.oryx.app.rdf.tree.DecisionTree}s. {@link Prediction}s from many
 * {@link com.cloudera.oryx.app.rdf.tree.TreeBasedClassifier}s are combined and weighted according to the accuracy of
 * the individuals.
 */
public final class WeightedPrediction {

  private WeightedPrediction() {}

  /**
   * @param predictions {@link Prediction}s from individuals
   * @param weights weights that should be applied to
   * @param <T> type of {@link Prediction} to vote on
   * @return a single {@link Prediction} represented a weighted combination of the inputs
   */
  public static <T extends Prediction> Prediction voteOnFeature(List<T> predictions,
                                                                double[] weights) {
    Preconditions.checkArgument(!predictions.isEmpty(), "No predictions");
    Preconditions.checkArgument(predictions.size() == weights.length,
                                "%s predictions but %s weights?", predictions.size(),
                                weights.length);
    switch (predictions.get(0).getFeatureType()) {
      case NUMERIC:
        @SuppressWarnings("unchecked")
        List<NumericPrediction> numericVotes = (List<NumericPrediction>) predictions;
        return voteOnNumericFeature(numericVotes, weights);
      case CATEGORICAL:
        @SuppressWarnings("unchecked")
        List<CategoricalPrediction> categoricalVotes = (List<CategoricalPrediction>) predictions;
        return voteOnCategoricalFeature(categoricalVotes, weights);
      default:
        throw new IllegalStateException();
    }
  }
  
  private static Prediction voteOnCategoricalFeature(List<CategoricalPrediction> predictions,
                                                     double[] weights) {
    double[] weightedProbabilities = null;
    double totalWeight = 0.0;
    for (int i = 0; i < predictions.size(); i++) {
      CategoricalPrediction vote = predictions.get(i);
      double weight = weights[i];
      totalWeight += weight;
      double[] categoryProbabilities = vote.getCategoryProbabilities();
      if (weightedProbabilities == null) {
        weightedProbabilities = new double[categoryProbabilities.length];
      }
      for (int j = 0; j < weightedProbabilities.length; j++) {
        weightedProbabilities[j] += categoryProbabilities[j] * weight;
      }
    }
    Objects.requireNonNull(weightedProbabilities, "No predictions?");
    for (int j = 0; j < weightedProbabilities.length; j++) {
      weightedProbabilities[j] /= totalWeight;
    }
    return new CategoricalPrediction(weightedProbabilities);
  }
  
  private static Prediction voteOnNumericFeature(List<NumericPrediction> predictions,
                                                 double[] weights) {
    DoubleWeightedMean mean = new DoubleWeightedMean();
    for (int i = 0; i < predictions.size(); i++) {
      mean.increment(predictions.get(i).getPrediction(), weights[i]);
    }
    return new NumericPrediction(mean.getResult(), (int) mean.getN());
  }

}
