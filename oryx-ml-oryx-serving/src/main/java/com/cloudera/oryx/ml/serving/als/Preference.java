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

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * <p>Responds to a POST request to {@code /pref/[userID]/[itemID]} and in turn calls
 * {link OryxRecommender#setPreference(String, String, float)}. If the request body is empty,
 * the value is 1.0, otherwise the value in the request body's first line is used.</p>
 *
 * <p>Also responds to a DELETE request to the same path, with the same defaults. This corresponds
 * to calling {link OryxRecommender#removePreference(String, String)} instead.</p>
 */
@Path("/pref")
public final class Preference extends AbstractALSResource {

  @GET
  @Path("{userID}/{itemID}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response get(@PathParam("userID") String userID,
                      @PathParam("itemID") String itemID) {
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

    float prefValue;
    try {
      prefValue = readValue(request);
    } catch (IllegalArgumentException ignored) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Bad value");
      return;
    }

    OryxRecommender recommender = getRecommender();
    recommender.setPreference(userID, itemID, prefValue);
  */
    return Response.status(200).entity("").build();
  }

}
