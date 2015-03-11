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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.cloudera.oryx.app.rdf.predict.CategoricalPrediction;
import com.cloudera.oryx.app.rdf.predict.Prediction;
import com.cloudera.oryx.app.schema.CategoricalValueEncodings;
import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.app.serving.CSVMessageBodyWriter;
import com.cloudera.oryx.app.serving.IDValue;
import com.cloudera.oryx.app.serving.OryxServingException;
import com.cloudera.oryx.app.serving.rdf.model.RDFServingModel;
import com.cloudera.oryx.common.text.TextUtils;

/**
 * <p>Responds to a GET request to {@code /classificationDistribution/[datum]}.
 * Like {@link Predict} but this returns not just the most probable category,
 * but all categories and their associated probability. It is not defined for
 * regression problems and returns an error.</p>
 *
 * <p>Default output is CSV format, containing {@code category,probability} per line.
 * JSON format can also be selected by an appropriate {@code Accept} header. It returns
 * an array of probabilities, each of which has an "id" and "value" entry, like
 * [{"id":"I2","value":0.141348009071816},...]</p>
 */
@Path("/classificationDistribution")
public final class ClassificationDistribution extends AbstractRDFResource {

  @GET
  @Path("{datum}")
  @Produces({MediaType.TEXT_PLAIN, CSVMessageBodyWriter.TEXT_CSV, MediaType.APPLICATION_JSON})
  public List<IDValue> get(@PathParam("datum") String datum) throws OryxServingException {
    check(datum != null && !datum.isEmpty(), "Missing input data");
    RDFServingModel model = getRDFServingModel();
    InputSchema inputSchema = model.getInputSchema();
    check(inputSchema.isClassification(), "Only applicable for classification");

    Prediction prediction = makePrediction(TextUtils.parseDelimited(datum, ','));

    double[] probabilities = ((CategoricalPrediction) prediction).getCategoryProbabilities();
    int targetIndex = inputSchema.getTargetFeatureIndex();
    CategoricalValueEncodings valueEncodings = model.getEncodings();
    Map<Integer,String> targetEncodingName = valueEncodings.getEncodingValueMap(targetIndex);

    List<IDValue> result = new ArrayList<>(probabilities.length);
    for (int i = 0; i < probabilities.length; i++) {
      result.add(new IDValue(targetEncodingName.get(i), probabilities[i]));
    }
    return result;
  }

}
