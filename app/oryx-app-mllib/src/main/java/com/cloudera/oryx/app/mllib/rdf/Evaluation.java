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

package com.cloudera.oryx.app.mllib.rdf;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;

import com.cloudera.oryx.app.rdf.example.CategoricalFeature;
import com.cloudera.oryx.app.rdf.example.Example;
import com.cloudera.oryx.app.rdf.example.NumericFeature;
import com.cloudera.oryx.app.rdf.predict.CategoricalPrediction;
import com.cloudera.oryx.app.rdf.predict.NumericPrediction;
import com.cloudera.oryx.app.rdf.tree.DecisionForest;

final class Evaluation {

  private Evaluation() {
  }

  static double rmse(final DecisionForest forest, JavaRDD<Example> examples) {
    double mse = examples.mapToDouble(
        new DoubleFunction<Example>() {
          @Override
          public double call(Example example) {
            NumericPrediction prediction = (NumericPrediction) forest.predict(example);
            NumericFeature target = (NumericFeature) example.getTarget();
            double diff = prediction.getPrediction() - target.getValue();
            return diff * diff;
          }
        }).mean();
    return Math.sqrt(mse);
  }

  static double accuracy(final DecisionForest forest, JavaRDD<Example> examples) {
    long total = examples.count();
    long correct = examples.filter(
        new Function<Example, Boolean>() {
          @Override
          public Boolean call(Example example) {
            CategoricalPrediction prediction = (CategoricalPrediction) forest.predict(example);
            CategoricalFeature target = (CategoricalFeature) example.getTarget();
            return prediction.getMostProbableCategoryEncoding() == target.getEncoding();
          }
        }).count();
    return (double) correct / total;
  }

}