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
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * <p>Responds to a GET request to {@code /item/allIDs}
 * and in turn calls {link com.cloudera.oryx.als.common.OryxRecommender#getAllItemIDs()}.</p>
 * <p/>
 * <p>CSV output is one item ID per line. JSON output is an array of item IDs.</p>
 */
@Path("/item")
public final class AllItemIDs {

  private static final Logger LOG = LoggerFactory.getLogger(AllItemIDs.class);

  @GET
  @Path("/allIDs")
  @Produces({MediaType.APPLICATION_JSON})
  public Response get() {
/*
    OryxRecommender recommender = getRecommender();
    try {
      output(request, response, recommender.getAllItemIDs());
    } catch (NotReadyException nre) {
      response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, nre.toString());
    }
 */
    return Response.status(200).entity("").build();
  }
}
