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

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

/**
 * <p>Responds to a GET request to {@code /mostPopularItems(?howMany=n)(&offset=o)}
 * and in turn calls {link OryxRecommender#mostPopularItems(int)}.
 * {@code offset} is an offset into the entire list of results; {@code howMany} is the desired
 * number of results to return from there. For example, {@code offset=30} and {@code howMany=5}
 * will cause the implementation to retrieve 35 results internally and output the last 5.
 * If {@code howMany} is not specified, defaults to {link AbstractALSServlet#DEFAULT_HOW_MANY}.
 * {@code offset} defaults to 0.</p>
 *
 * <p>Output is as in {@link Recommend}.</p>
 *
 */
@Path("/mostPopularItems")
public class MostPopularItems {

    @GET
    @Path("{userId}")
    @Produces({MediaType.APPLICATION_JSON})
    public Response get(@PathParam("userId") String userId, @QueryParam("howMany") int howMany,
                        @QueryParam("offset") int offset,
                        @QueryParam("considerKnownItems") boolean considerKnownItems,
                        @QueryParam("rescorerParams") List<String> rescorerParams) {
/*
    OryxRecommender recommender = getRecommender();
    RescorerProvider rescorerProvider = getRescorerProvider();
    try {
      Rescorer rescorer = rescorerProvider == null ? null :
          rescorerProvider.getMostPopularItemsRescorer(recommender, getRescorerParams(request));
      outputALSResult(request, response, recommender.mostPopularItems(getNumResultsToFetch(request), rescorer));
    } catch (NotReadyException nre) {
      response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, nre.toString());
    } catch (IllegalArgumentException iae) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, iae.toString());
    }
  */
        return Response.status(200).entity("").build();
    }

}
