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
import javax.ws.rs.core.PathSegment;
import java.util.Arrays;
import java.util.List;

/**
 * <p>Responds to a GET request to
 * {@code /estimateForAnonymous/[toItemID]/[itemID1(=value1)](/[itemID2(=value2)]/...)},
 * and in turn calls {link OryxRecommender#estimateForAnonymous(String, String[], float[])}
 * with the supplied values. That is, 1 or more item IDs are supplied, which may each optionally correspond to
 * a value or else default to 1.</p>
 * <p/>
 * <p>Unknown item IDs are ignored, unless all are unknown, in which case a
 * {link HttpServletResponse#SC_BAD_REQUEST} status is returned.</p>
 * <p/>
 * <p>Outputs the result of the method call as a value on one line.</p>
 */
@Path("/estimateForAnonymous")
public final class EstimateForAnonymous {

  @GET
  @Path("{toItemID}")
  @Produces(MediaType.APPLICATION_JSON)
  public List<Float> get(@PathParam("toItemID") String toItemID,
                         @PathParam("itemID") PathSegment items) {
/*
    CharSequence pathInfo = request.getPathInfo();
    if (pathInfo == null) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No path");
      return;
    }
    Iterator<String> pathComponents = SLASH.split(pathInfo).iterator();
    String toItemID;
    Pair<String[],float[]> itemIDsAndValue;
    try {
      toItemID = pathComponents.next();
      itemIDsAndValue = RecommendToAnonymousServlet.parseItemValuePairs(pathComponents);
    } catch (NoSuchElementException nsee) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, nsee.toString());
      return;
    }

    if (itemIDsAndValue.getFirst().length == 0) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No items");
      return;
    }

    String[] itemIDs = itemIDsAndValue.getFirst();
    unescapeSlashHack(itemIDs);
    float[] values = itemIDsAndValue.getSecond();

    OryxRecommender recommender = getRecommender();
    try {
      float estimate = recommender.estimateForAnonymous(toItemID, itemIDs, values);
      output(request, response, new float[] { estimate });
    } catch (NotReadyException nre) {
      response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, nre.toString());
    } catch (NoSuchItemException nsie) {
      response.sendError(HttpServletResponse.SC_NOT_FOUND, nsie.toString());
    }
  */
    return Arrays.asList(1.2f, 3.4f);
  }

}
