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

package com.cloudera.oryx.app.rdf.tree;

import java.util.Arrays;
import java.util.Iterator;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import com.cloudera.oryx.app.rdf.predict.Prediction;
import com.cloudera.oryx.app.rdf.example.Example;
import com.cloudera.oryx.app.rdf.predict.WeightedPrediction;

/**
 * An ensemble classifier based on many {@link DecisionTree}s.
 *
 * @see DecisionTree
 */
public final class DecisionForest implements Iterable<DecisionTree>, TreeBasedClassifier {

  private final DecisionTree[] trees;
  private final double[] weights;
  private final double[] evaluations;
  private final double[] featureImportances;

  public DecisionForest(DecisionTree[] trees, double[] weights, double[] featureImportances) {
    this.trees = trees;
    this.weights = weights;
    this.evaluations = new double[weights.length];
    this.featureImportances = featureImportances;
  }

  @Override
  public Iterator<DecisionTree> iterator() {
    return Arrays.asList(trees).iterator();
  }

  /**
   * @return {@link DecisionTree}s in the ensemble forest
   */
  public DecisionTree[] getTrees() {
    return trees;
  }

  public double[] getWeights() {
    return weights;
  }

  public double[] getEvaluations() {
    return evaluations;
  }

  public double[] getFeatureImportances() {
    return featureImportances;
  }
  
  @Override
  public Prediction classify(Example test) {
    return WeightedPrediction.voteOnFeature(
        Lists.transform(Arrays.asList(trees), new TreeToPredictionFunction(test)), weights);
  }

  @Override
  public void update(Example train) {
    for (TreeBasedClassifier tree : trees) {
      tree.update(train);
    }
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    for (DecisionTree tree : trees) {
      result.append(tree).append('\n');
    }
    return result.toString();
  }

  private static final class TreeToPredictionFunction implements Function<DecisionTree, Prediction> {
    private final Example test;
    TreeToPredictionFunction(Example test) {
      this.test = test;
    }
    @Override
    public Prediction apply(DecisionTree tree) {
      return tree.classify(test);
    }
  }
}
