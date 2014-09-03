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

package com.cloudera.oryx.ml.serving.als;

import java.util.Arrays;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.cloudera.oryx.ml.serving.ErrorResponse;
import com.cloudera.oryx.ml.serving.IDValue;

/**
 * <p>Responds to a GET request to
 * {@code /recommendToAnonymous/[itemID1(=value1)](/[itemID2(=value2)]/...)(?howMany=n)(&offset=o)(&rescorerParams=...)},
 * and in turn calls {link OryxRecommender#recommendToAnonymous(String[], float[], int, Rescorer)}
 * with the supplied values. That is, 1 or more item IDs are supplied, which may each optionally correspond to
 * a value or else default to 1.
 * {@code offset} is an offset into the entire list of results; {@code howMany} is the desired
 * number of results to return from there. For example, {@code offset=30} and {@code howMany=5}
 * will cause the implementation to retrieve 35 results internally and output the last 5.
 * If {@code howMany} is not specified, defaults to {link AbstractALSServlet#DEFAULT_HOW_MANY}.
 * {@code offset} defaults to 0.</p>
 *
 * <p>Unknown item IDs are ignored, unless all are unknown, in which case a
 * {link HttpServletResponse#SC_BAD_REQUEST} status is returned.</p>
 *
 * <p>Outputs item/score pairs like {@link Recommend} does.</p>
 *
 * <p>This does something slightly different from {@link Similarity};
 * see {link OryxRecommender#recommendToAnonymous(String[], float[], int)}.</p>
 */
@Path("/recommendToAnonymous")
public final class RecommendToAnonymous extends AbstractALSResource {

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response get() {
    return Response.status(Response.Status.BAD_REQUEST).entity(
        new ErrorResponse(Response.Status.BAD_REQUEST, "One or more item IDs required")).build();
  }

  @GET
  @Path("{itemID}")
  @Produces(MediaType.APPLICATION_JSON)
  public List<IDValue> get(@PathParam("itemID") String itemID,
                                     @QueryParam("howMany") int howMany,
                                     @QueryParam("offset") int offset,
                                     @QueryParam("considerKnownItems") boolean considerKnownItems,
                                     @QueryParam("rescorerParams") List<String> rescorerParams) {
/*
    CharSequence pathInfo = request.getPathInfo();
    if (pathInfo == null) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No path");
      return;
    }
    Iterator<String> pathComponents = SLASH.split(pathInfo).iterator();
    Pair<String[],float[]> itemIDsAndValue;
    try {
      itemIDsAndValue = parseItemValuePairs(pathComponents);
    } catch (NoSuchElementException nsee) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, nsee.toString());
      return;
    }

    if (itemIDsAndValue.getFirst().length == 0) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No items");
      return;
    }

    String[] itemIDs = itemIDsAndValue.getFirst();
    float[] values = itemIDsAndValue.getSecond();

    OryxRecommender recommender = getRecommender();
    RescorerProvider rescorerProvider = getRescorerProvider();
    try {
      Rescorer rescorer = rescorerProvider == null ? null :
          rescorerProvider.getRecommendToAnonymousRescorer(itemIDs, recommender, getRescorerParams(request));
      outputALSResult(request,
                      response,
                      recommender.recommendToAnonymous(itemIDs, values, getNumResultsToFetch(request), rescorer));
    } catch (NotReadyException nre) {
      response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, nre.toString());
    } catch (NoSuchItemException nsie) {
      response.sendError(HttpServletResponse.SC_NOT_FOUND, nsie.toString());
    } catch (IllegalArgumentException iae) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, iae.toString());
    }
  */
    return Arrays.asList(new IDValue("1", 5));
  }

}
