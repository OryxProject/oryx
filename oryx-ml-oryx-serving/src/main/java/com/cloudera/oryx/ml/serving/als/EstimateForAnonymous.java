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
import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;

import com.cloudera.oryx.lambda.serving.ErrorResponse;

/**
 * <p>Responds to a GET request to
 * {@code /estimateForAnonymous/[toItemID]/[itemID1(=value1)](/[itemID2(=value2)]/...)}.
 * That is, 1 or more item IDs are supplied, which may each optionally correspond to
 * a value or else default to 1.</p>
 *
 * <p>Unknown item IDs are ignored, unless all are unknown, in which case a
 * {link HttpServletResponse#SC_BAD_REQUEST} status is returned.</p>
 *
 * <p>Outputs the result of the method call as a value on one line.</p>
 */
@Path("/estimateForAnonymous")
public final class EstimateForAnonymous {

  @Context
  private ServletContext servletContext;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getNoArgs() {
    return Response.status(Response.Status.BAD_REQUEST.getStatusCode()).entity(new ErrorResponse(Response.Status.BAD_REQUEST.getStatusCode(), "path /{toItemID}/{itemId}+ required")).build();
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("{toItemID}/{itemID : .+}")
  public List<Double> get(@PathParam("toItemID") String toItemID,
                                               @PathParam("itemID") List<PathSegment> pathSegmentList) {


    return Arrays.asList(1.2, 3.4);
  }

}
