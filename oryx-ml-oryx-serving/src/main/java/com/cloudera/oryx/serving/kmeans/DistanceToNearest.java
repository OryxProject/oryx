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

package com.cloudera.oryx.serving.kmeans;

import javax.ws.rs.GET;
import javax.ws.rs.MatrixParam;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * <p>Responds to a GET request to {@code /distanceToNearest/[datum]}. The input is one data point to cluster,
 * delimited, like "1,-4,3.0". The response body contains the distance to the nearest cluster, on one line.</p>
 */
@Path("/distanceToNearest")
public final class DistanceToNearest {

  @GET
  @Path("{datum}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response post(@MatrixParam("datum") String datum) {
/*
    CharSequence pathInfo = request.getPathInfo();
    if (pathInfo == null) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No path");
      return;
    }
    String line = pathInfo.subSequence(1, pathInfo.length()).toString();
    Generation generation = getGenerationManager().getCurrentGeneration();
    if (generation == null) {
      response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE,
                         "API method unavailable until model has been built and loaded");
      return;
    }

    RealVector vec = generation.toVector(DelimitedDataUtils.decode(line));
    if (vec == null) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Wrong column count");
      return;
    }

    int assignment = DistanceToNearestServlet.findClosest(generation, vec).getClosestCenterId();
    response.getWriter().write(Integer.toString(assignment));
  */
    return Response.status(200).entity("").build();
  }

}
