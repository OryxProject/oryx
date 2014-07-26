/*
 * Copyright (c) 2014, Cloudera, Inc. and Intel Corp. All Rights Reserved.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * <p>Responds to a GET request to {@code /estimate/[userID]/[itemID]} and in turn calls
 * {link OryxRecommender#estimatePreference(String, String)}.</p>
 * <p/>
 * <p>Outputs the result of the method call as a value on one line.</p>
 * <p/>
 * <p>This servlet can also compute several estimates at once. Send a GET request to
 * {@code /estimate/[userID]/[itemID1](/[itemID2]/...)}. The output are estimates, in the same
 * order as the item ID, one per line.</p>
 */
@Path("/estimate")
public class Estimate {

  private static final Logger LOG = LoggerFactory.getLogger(Estimate.class);

  @GET
  @Path("{userID}/{itemID}")
  @Produces({MediaType.APPLICATION_JSON})
  public Response get(@PathParam("userID") String userID, @PathParam("itemID") String itemID) {
/*
    CharSequence pathInfo = request.getPathInfo();
    if (pathInfo == null) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No path");
      return;
    }
    Iterator<String> pathComponents = SLASH.split(pathInfo).iterator();
    String userID;
    List<String> itemIDsList;
    try {
      userID = pathComponents.next();
      itemIDsList = Lists.newArrayList();
      while (pathComponents.hasNext()) {
        itemIDsList.add(pathComponents.next());
      }
    } catch (NoSuchElementException nsee) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, nsee.toString());
      return;
    }

    String[] itemIDs = itemIDsList.toArray(new String[itemIDsList.size()]);
    unescapeSlashHack(itemIDs);

    OryxRecommender recommender = getRecommender();
    try {
      float[] estimates = recommender.estimatePreferences(userID, itemIDs);
      output(request, response, estimates);
    } catch (NotReadyException nre) {
      response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, nre.toString());
    }
  */
    return Response.status(200).entity("").build();
  }

}
