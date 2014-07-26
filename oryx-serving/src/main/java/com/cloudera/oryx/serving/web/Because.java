/*
 * Copyright (c) 2014, Cloudera, Inc. AND Intel Corp. All Rights Reserved.
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

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * <p>Responds to a GET request to {@code /because/[userID]/[itemID](?howMany=n)(&offset=o)},
 * and in turn calls {link OryxRecommender#recommendedBecause(String, String, int)}.
 * {@code offset} is an offset into the entire list of results; {@code howMany} is the desired
 * number of results to return from there. For example, {@code offset=30} and {@code howMany=5}
 * will cause the implementation to retrieve 35 results internally and output the last 5.
 * If {@code howMany} is not specified, defaults to {link AbstractALSServlet#DEFAULT_HOW_MANY}.
 * {@code offset} defaults to 0.</p>
 * <p/>
 * <p>Outputs item/score pairs like {@link Recommend} does.</p>
 */
@Path("/because")
public class Because {

  @GET
  @Path("{userId}/{itemId}")
  @Produces({MediaType.APPLICATION_JSON})
  public Response get(@PathParam("userId") String userId, @PathParam("itemId") String itemId,
                      @QueryParam("howMany") int howMany, @QueryParam("offset") int offset) {
/*
    CharSequence pathInfo = request.getPathInfo();
    if (pathInfo == null) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No path");
      return;
    }
    Iterator<String> pathComponents = SLASH.split(pathInfo).iterator();
    String userID;
    String itemID;
    try {
      userID = pathComponents.next();
      itemID = pathComponents.next();
    } catch (NoSuchElementException nsee) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, nsee.toString());
      return;
    }
    if (pathComponents.hasNext()) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Path too long");
      return;
    }

    userID = unescapeSlashHack(userID);
    itemID = unescapeSlashHack(itemID);

    OryxRecommender recommender = getRecommender();
    try {
      outputALSResult(request,
                      response,
                      recommender.recommendedBecause(userID, itemID, getNumResultsToFetch(request)));
    } catch (NoSuchUserException nsue) {
      response.sendError(HttpServletResponse.SC_NOT_FOUND, nsue.toString());
    } catch (NoSuchItemException nsie) {
      response.sendError(HttpServletResponse.SC_NOT_FOUND, nsie.toString());
    } catch (NotReadyException nre) {
      response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, nre.toString());
    } catch (IllegalArgumentException iae) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, iae.toString());
    } catch (UnsupportedOperationException uoe) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, uoe.toString());
    }
 */
    return Response.status(200).entity("").build();
  }

}
