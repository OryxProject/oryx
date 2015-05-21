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

import javax.annotation.PostConstruct;
import javax.ws.rs.core.Response;

import com.cloudera.oryx.app.rdf.example.CategoricalFeature;
import com.cloudera.oryx.app.rdf.example.Example;
import com.cloudera.oryx.app.rdf.example.Feature;
import com.cloudera.oryx.app.rdf.example.NumericFeature;
import com.cloudera.oryx.app.rdf.predict.Prediction;
import com.cloudera.oryx.app.schema.CategoricalValueEncodings;
import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.app.serving.AbstractOryxResource;
import com.cloudera.oryx.app.serving.OryxServingException;
import com.cloudera.oryx.app.serving.rdf.model.RDFServingModel;

/**
 * Superclass of all Serving Layer random decision forest application endpoints.
 */
public abstract class AbstractRDFResource extends AbstractOryxResource {

  private RDFServingModel rdfServingModel;

  @Override
  @PostConstruct
  public void init() {
    super.init();
    rdfServingModel = (RDFServingModel) getServingModelManager().getModel();
  }

  final RDFServingModel getRDFServingModel() throws OryxServingException {
    if (rdfServingModel == null) {
      throw new OryxServingException(Response.Status.SERVICE_UNAVAILABLE);
    }
    return rdfServingModel;
  }

  Prediction makePrediction(String[] data) throws OryxServingException {
    CategoricalValueEncodings valueEncodings = rdfServingModel.getEncodings();
    InputSchema inputSchema = rdfServingModel.getInputSchema();
    check(data.length == inputSchema.getNumFeatures(), "Wrong number of features");

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

    return rdfServingModel.getForest().predict(new Example(target, features));
  }

}
