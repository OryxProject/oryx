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

import com.cloudera.oryx.app.rdf.example.Example;
import com.cloudera.oryx.app.rdf.example.ExampleUtils;
import com.cloudera.oryx.app.rdf.predict.Prediction;
import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.app.serving.AbstractOryxResource;
import com.cloudera.oryx.app.serving.OryxServingException;
import com.cloudera.oryx.app.serving.rdf.model.RDFServingModel;

/**
 * Superclass of all Serving Layer random decision forest application endpoints.
 */
public abstract class AbstractRDFResource extends AbstractOryxResource {

  final RDFServingModel getRDFServingModel() throws OryxServingException {
    return (RDFServingModel) getServingModel();
  }

  Prediction makePrediction(String[] data) throws OryxServingException {
    RDFServingModel rdfServingModel = getRDFServingModel();
    InputSchema inputSchema = rdfServingModel.getInputSchema();
    check(data.length == inputSchema.getNumFeatures(), "Wrong number of features");
    Example example = ExampleUtils.dataToExample(data, inputSchema, rdfServingModel.getEncodings());
    return rdfServingModel.getForest().predict(example);
  }

}
