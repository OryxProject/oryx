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

import com.google.common.collect.Lists;

import com.cloudera.oryx.app.classreg.predict.Prediction;
import com.cloudera.oryx.app.classreg.example.Example;
import com.cloudera.oryx.app.classreg.predict.WeightedPrediction;

/**
 * An ensemble classifier based on many {@link DecisionTree}s.
 *
 * @see DecisionTree
 */
public final class DecisionForest implements TreeBasedClassifier {

  private final DecisionTree[] trees;
  private final double[] weights;
  //private final double[] evaluations;
  private final double[] featureImportances;

  public DecisionForest(DecisionTree[] trees, double[] weights, double[] featureImportances) {
    this.trees = trees;
    this.weights = weights;
    //this.evaluations = new double[weights.length];
    this.featureImportances = featureImportances;
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

  /*
  public double[] getEvaluations() {
    return evaluations;
  }
   */

  public double[] getFeatureImportances() {
    return featureImportances;
  }
  
  @Override
  public Prediction predict(Example test) {
    return WeightedPrediction.voteOnFeature(
        Lists.transform(Arrays.asList(trees), tree -> tree.predict(test)), weights);
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

}
