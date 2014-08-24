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

import java.io.BufferedReader;
import java.io.IOException;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.cloudera.oryx.ml.serving.als.model.ALSServingModel;

/**
 * <p>Responds to a POST request to {@code /pref/[userID]/[itemID]} and in turn calls
 * {link ALSServingModel#setPreference(String, String, float)}. If the request body is empty,
 * the value is 1.0, otherwise the value in the request body's first line is used.</p>
 *
 * <p>Also responds to a DELETE request to the same path, with the same defaults. This corresponds
 * to calling {link ALSServingModel#removePreference(String, String)} instead.</p>
 */
@Path("/pref")
public final class Preference extends AbstractALSResource {

  @Context
  private HttpServletRequest httpServletRequest;

  @POST
  @Path("{userID}/{itemID}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response post(@PathParam("userID") String userID,
                       @PathParam("itemID") String itemID) {
    ALSServingModel alsServingModel = getALSServingModel();
    float preferenceValue;
    try {
      preferenceValue = readValue(httpServletRequest);
    } catch (IllegalArgumentException | IOException ignored) {
      return Response.status(Response.Status.BAD_REQUEST).entity("Bad value").build();
    }
    // TODO: Need to code this up
    /* Call ALSServingModel.setPreference() */
    return Response.ok().entity("").build();
  }

  @DELETE
  @Path("{userID}/{itemID}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response delete(@PathParam("userID") String userID,
                         @PathParam("itemID") String itemID) {
    ALSServingModel alsServingModel = getALSServingModel();

    // TODO: Need to code this up
    /* Call ALSServingModel.removePreference() */
    return Response.ok().entity("").build();
  }

  private static float readValue(final HttpServletRequest httpServletRequest) throws IOException {
    String line;
    try (BufferedReader reader = httpServletRequest.getReader()) {
      line = reader.readLine();
    }
    if (line == null || line.isEmpty()) {
      return 1.0f;
    }
    return Float.parseFloat(line);
  }

}
