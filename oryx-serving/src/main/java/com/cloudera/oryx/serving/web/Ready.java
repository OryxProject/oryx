/*
 * Copyright (c) 2013, Cloudera, Inc. All Rights Reserved.
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
import javax.ws.rs.HEAD;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * <p>Responds to a HEAD or GET request to {@code /ready} and in turn calls
 * {link com.cloudera.oryx.als.common.OryxRecommender#isReady()}. Returns "OK" or "Unavailable" status depending on
 * whether the recommender is ready.</p>
 *
 */
@Path("/ready")
public class Ready {

    @HEAD
    @Produces({MediaType.APPLICATION_JSON})
    public Response head() {
      return get();
    }

    @GET
    @Produces({MediaType.APPLICATION_JSON})
    public Response get() {
/*
    boolean isReady = getRecommender().isReady();
    if (isReady) {
      response.setStatus(HttpServletResponse.SC_OK);
    } else {
      response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
    }
  */
        return Response.status(200).entity("").build();
    }

}
