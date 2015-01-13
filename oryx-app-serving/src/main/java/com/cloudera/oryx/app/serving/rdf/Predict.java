/*
 * Copyright (c) 2014, Cloudera and Intel, Inc. All Rights Reserved.
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

package com.cloudera.oryx.app.serving.rdf;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import java.util.Map;

import com.cloudera.oryx.app.rdf.example.CategoricalFeature;
import com.cloudera.oryx.app.rdf.example.Example;
import com.cloudera.oryx.app.rdf.example.Feature;
import com.cloudera.oryx.app.rdf.example.NumericFeature;
import com.cloudera.oryx.app.rdf.predict.CategoricalPrediction;
import com.cloudera.oryx.app.rdf.predict.NumericPrediction;
import com.cloudera.oryx.app.rdf.predict.Prediction;
import com.cloudera.oryx.app.schema.CategoricalValueEncodings;
import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.app.serving.CSVMessageBodyWriter;
import com.cloudera.oryx.app.serving.OryxServingException;
import com.cloudera.oryx.app.serving.rdf.model.RDFServingModel;
import com.cloudera.oryx.common.text.TextUtils;

/**
 * <p>Responds to a GET request to {@code /predict/[datum]}. The input is one data point to predict,
 * delimited, like "1,foo,3.0". The value of the target feature in the input is ignored.
 * The response body contains the result of prediction on one line.
 * The result depends on the classifier or regressor --  could be a number or a category name.</p>
 */
@Path("/predict")
public final class Predict extends AbstractRDFResource {

  @GET
  @Path("{datum}")
  @Produces({MediaType.TEXT_PLAIN, CSVMessageBodyWriter.TEXT_CSV, MediaType.APPLICATION_JSON})
  public String get(@PathParam("datum") String datum) throws OryxServingException {
    check(datum != null && !datum.isEmpty(), "Missing input data");
    RDFServingModel model = getRDFServingModel();
    InputSchema inputSchema = model.getInputSchema();

    Prediction prediction = makePrediction(TextUtils.parseCSV(datum), model);

    String result;
    if (inputSchema.isClassification()) {
      int targetIndex = inputSchema.getTargetFeatureIndex();
      CategoricalValueEncodings valueEncodings = model.getEncodings();
      Map<Integer,String> targetEncodingName = valueEncodings.getEncodingValueMap(targetIndex);
      int mostProbable = ((CategoricalPrediction) prediction).getMostProbableCategoryEncoding();
      result = targetEncodingName.get(mostProbable);
    } else {
      double score = ((NumericPrediction) prediction).getPrediction();
      result = Double.toString(score);
    }
    return result;
  }

  static Prediction makePrediction(String[] data, RDFServingModel model) {
    CategoricalValueEncodings valueEncodings = model.getEncodings();
    InputSchema inputSchema = model.getInputSchema();

    Feature[] features = new Feature[data.length];
    Feature target = null;
    for (int featureIndex = 0; featureIndex < data.length; featureIndex++) {
      Feature feature = null;
      String dataAtIndex = data[featureIndex];
      boolean isTarget = inputSchema.isTarget(featureIndex);
      if (isTarget && dataAtIndex.isEmpty()) {
        feature = null;
      } else if (inputSchema.isNumeric(featureIndex)) {
        feature = NumericFeature.forValue(Double.parseDouble(dataAtIndex));
      } else if (inputSchema.isCategorical(featureIndex)) {
        int encoding = valueEncodings.getValueEncodingMap(featureIndex).get(dataAtIndex);
        feature = CategoricalFeature.forEncoding(encoding);
      }
      if (isTarget) {
        target = feature;
      } else {
        features[featureIndex] = feature;
      }
    }

    return model.getForest().predict(new Example(target, features));
  }

}
