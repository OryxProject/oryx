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

package com.cloudera.oryx.app.serving.rdf;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;

import com.cloudera.oryx.api.serving.OryxServingException;
import com.cloudera.oryx.app.serving.rdf.model.RDFServingModel;

/**
 * <p>Responds to a GET request to {@code /feature/importance} or
 * {@code /feature/importance/[feature number]}. In the first case, the results are feature
 * importance values for all features in order as specified in the configuration.
 * In the second case, specified by number, the result is a single feature importance.</p>
 *
 * <p>In both cases, importance is given as a floating point value between 0 and 1 inclusive.
 * Higher means more important. The values are normalized and are not expressed in
 * particular units.</p>
 *
 * <p>The result contains one importance value per line. If JSON output is selected, the result
 * is a JSON list of importance values.</p>
 */
@Singleton
@Path("/feature/importance")
public final class FeatureImportance extends AbstractRDFResource {

  @GET
  @Produces({MediaType.TEXT_PLAIN, "text/csv", MediaType.APPLICATION_JSON})
  public List<Double> getAllImportances() throws OryxServingException {
    RDFServingModel model = getRDFServingModel();
    double[] importances = model.getForest().getFeatureImportances();
    List<Double> result = new ArrayList<>(importances.length);
    for (double importance : importances) {
      result.add(importance);
    }
    return result;
  }

  @GET
  @Produces({MediaType.TEXT_PLAIN, "text/csv", MediaType.APPLICATION_JSON})
  @Path("{featureNumber}")
  public Double getImportance(@PathParam("featureNumber") int featureNumber)
      throws OryxServingException {
    RDFServingModel model = getRDFServingModel();
    double[] importances = model.getForest().getFeatureImportances();
    check(featureNumber >= 0 && featureNumber < importances.length, "Bad feature number");
    return importances[featureNumber];
  }

}
