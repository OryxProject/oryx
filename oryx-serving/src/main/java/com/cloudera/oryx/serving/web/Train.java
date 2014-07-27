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

package com.cloudera.oryx.serving.web;

import javax.ws.rs.MatrixParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * <p>Responds to POST request to {@code /train}. The input is one or more data points
 * to train, one for each line of the request body. Each data point is a delimited line of input like
 * "1,foo,3.0". The classifier updates to learn in some way from the new data. The response is empty.</p>
 */
@Path("/train")
public class Train {

  @POST
  @Path("{input}")
  @Produces({MediaType.APPLICATION_JSON})
  public Response post(@MatrixParam("input") String input) {
/*
    RDFGenerationManager generationManager = getGenerationManager();
    Generation generation = generationManager.getCurrentGeneration();
    if (generation == null) {
      response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE,
                         "API method unavailable until model has been built and loaded");
      return;
    }

    InboundSettings inboundSettings = getInboundSettings();

    TreeBasedClassifier forest = generation.getForest();
    Map<Integer,BiMap<String,Integer>> columnToCategoryNameToIDMapping =
        generation.getColumnToCategoryNameToIDMapping();

    int totalColumns = getTotalColumns();

    for (CharSequence line : CharStreams.readLines(request.getReader())) {

      generationManager.append(line);

      String[] tokens = DelimitedDataUtils.decode(line);
      if (tokens.length != totalColumns) {
        response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Wrong column count");
        return;
      }

      Feature target = null;
      Feature[] features = new Feature[totalColumns]; // Too big by 1 but makes math easier
      try {
        for (int col = 0; col < features.length; col++) {
          if (col == inboundSettings.getTargetColumn()) {
            target = buildFeature(col, tokens[col], columnToCategoryNameToIDMapping);
            features[col] = IgnoredFeature.INSTANCE;
          } else {
            features[col] = buildFeature(col, tokens[col], columnToCategoryNameToIDMapping);
          }
        }
      } catch (IllegalArgumentException iae) {
        response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Bad input line");
        return;
      }

      Preconditions.checkNotNull(target);
      Example example = new Example(target, features);

      forest.update(example);
  */
    return Response.status(200).entity("").build();
  }

}
