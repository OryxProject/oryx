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

package com.cloudera.oryx.ml.serving.kmeans;

import javax.ws.rs.MatrixParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * <p>Responds to POST request to {@code /add}. The input is one or more data points
 * to add to the clustering, one for each line of the request body. Each data point is a delimited line of input like
 * "1,-4,3.0". The clusters update to learn in some way from the new data. The response is empty.</p>
 */
@Path("/add")
public final class Add {

  @POST
  @Path("{input}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response get(@MatrixParam("input") String input) {
/*
    KMeansGenerationManager generationManager = getGenerationManager();
    Generation generation = generationManager.getCurrentGeneration();
    if (generation == null) {
      response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE,
                         "API method unavailable until model has been built and loaded");
      return;
    }

    for (CharSequence line : CharStreams.readLines(request.getReader())) {
      RealVector vec = generation.toVector(DelimitedDataUtils.decode(line));
      if (vec == null) {
        response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Wrong column count");
        return;
      }
      generationManager.append(line);

    }
  */
    return Response.status(200).entity("").build();
  }

}
