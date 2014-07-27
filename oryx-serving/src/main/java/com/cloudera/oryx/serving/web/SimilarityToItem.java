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

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * <p>Responds to a GET request to {@code /similarityToItem/[toItemID]/itemID1(/[itemID2]/...)},
 * and in turn calls {link OryxRecommender#similarityToItem(String, String...)} with the supplied values.</p>
 * <p/>
 * <p>Unknown item IDs are ignored, unless all are unknown or {@code toItemID} is unknown, in which case a
 * {link HttpServletResponse#SC_BAD_REQUEST} status is returned.</p>
 * <p/>
 * <p>The output are similarities, in the same order as the item IDs, one per line.</p>
 */
@Path("/similarityToItem")
public class SimilarityToItem {

  @GET
  @Path("{itemID}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response get(@PathParam("itemID") String itemID) {
/*
    CharSequence pathInfo = request.getPathInfo();
    if (pathInfo == null) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No path");
      return;
    }
    Iterator<String> pathComponents = SLASH.split(pathInfo).iterator();
    String toItemID;
    List<String> itemIDsList = Lists.newArrayList();
    try {
      toItemID = pathComponents.next();
      while (pathComponents.hasNext()) {
        itemIDsList.add(pathComponents.next());
      }
    } catch (NoSuchElementException nsee) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, nsee.toString());
      return;
    }
    if (itemIDsList.isEmpty()) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No items");
      return;
    }

    toItemID = unescapeSlashHack(toItemID);
    String[] itemIDs = itemIDsList.toArray(new String[itemIDsList.size()]);
    unescapeSlashHack(itemIDs);

    OryxRecommender recommender = getRecommender();
    try {
      float[] similarities = recommender.similarityToItem(toItemID, itemIDs);
      output(request, response, similarities);
    } catch (NoSuchItemException nsie) {
      response.sendError(HttpServletResponse.SC_NOT_FOUND, nsie.toString());
    } catch (NotReadyException nre) {
      response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, nre.toString());
    }
  */
    return Response.status(200).entity("").build();
  }

}
