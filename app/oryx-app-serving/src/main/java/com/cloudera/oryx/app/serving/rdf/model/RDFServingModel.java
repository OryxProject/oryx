/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
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

package com.cloudera.oryx.app.serving.rdf.model;

import java.util.Map;
import java.util.Objects;

import com.cloudera.oryx.app.classreg.example.ExampleUtils;
import com.cloudera.oryx.app.classreg.predict.CategoricalPrediction;
import com.cloudera.oryx.app.classreg.predict.NumericPrediction;
import com.cloudera.oryx.app.classreg.predict.Prediction;
import com.cloudera.oryx.app.rdf.tree.DecisionForest;
import com.cloudera.oryx.app.schema.CategoricalValueEncodings;
import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.app.serving.classreg.model.ClassificationRegressionServingModel;

/**
 * Contains all data structures needed to serve queries for a
 * random decision forest-based classifier or regressor.
 */
public final class RDFServingModel implements ClassificationRegressionServingModel {

  private final DecisionForest forest;
  private final CategoricalValueEncodings encodings;
  private final InputSchema inputSchema;

  RDFServingModel(DecisionForest forest,
                  CategoricalValueEncodings encodings,
                  InputSchema inputSchema) {
    Objects.requireNonNull(forest);
    Objects.requireNonNull(encodings);
    Objects.requireNonNull(inputSchema);
    this.forest = forest;
    this.encodings = encodings;
    this.inputSchema = inputSchema;
  }

  @Override
  public String predict(String[] example) {
    Prediction prediction = makePrediction(example);
    if (inputSchema.isClassification()) {
      int targetIndex = inputSchema.getTargetFeatureIndex();
      Map<Integer,String> targetEncodingName = encodings.getEncodingValueMap(targetIndex);
      int mostProbable = ((CategoricalPrediction) prediction).getMostProbableCategoryEncoding();
      return targetEncodingName.get(mostProbable);
    } else {
      double score = ((NumericPrediction) prediction).getPrediction();
      return Double.toString(score);
    }
  }

  public Prediction makePrediction(String[] example) {
    if (example.length != inputSchema.getNumFeatures()) {
      throw new IllegalArgumentException("Wrong number of features");
    }
    return forest.predict(ExampleUtils.dataToExample(example, inputSchema, encodings));
  }

  public DecisionForest getForest() {
    return forest;
  }

  public CategoricalValueEncodings getEncodings() {
    return encodings;
  }

  public InputSchema getInputSchema() {
    return inputSchema;
  }

  @Override
  public float getFractionLoaded() {
    return 1.0f;
  }

  @Override
  public String toString() {
    return "RDFServingModel[numTrees:" + forest.getTrees().length + "]";
  }

}
