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
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import com.cloudera.oryx.lambda.QueueProducer;
import com.cloudera.oryx.ml.serving.CSVMessageBodyWriter;
import com.cloudera.oryx.ml.serving.OryxServingException;

/**
 * <p>Responds to a POST request to {@code /pref/[userID]/[itemID]}. If the request body is empty,
 * the value is 1.0, otherwise the value in the request body's first line is used.</p>
 *
 * <p>Also responds to a DELETE request to the same path, with the same defaults.</p>
 */
@Path("/pref")
public final class Preference extends AbstractALSResource {

  @POST
  @Path("{userID}/{itemID}")
  @Consumes({CSVMessageBodyWriter.TEXT_CSV, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  public void post(
      @PathParam("userID") String userID,
      @PathParam("itemID") String itemID,
      @Context HttpServletRequest httpServletRequest) throws IOException, OryxServingException {
    Float itemValue = readRequestData(httpServletRequest);
    check(itemValue != null, "Bad Request Data");
    sendToQueue(userID + "," + itemID + "," + itemValue);
  }

  @DELETE
  @Path("{userID}/{itemID}")
  @Consumes({CSVMessageBodyWriter.TEXT_CSV, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  public void delete(
      @PathParam("userID") String userID,
      @PathParam("itemID") String itemID) {
    sendToQueue(userID + "," + itemID);
  }

  private void sendToQueue(String preferenceData) {
    QueueProducer<?,String> inputQueue = getInputProducer();
    inputQueue.send(preferenceData);
  }

  private Float readRequestData(HttpServletRequest request) throws IOException {
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(request.getInputStream(), StandardCharsets.UTF_8));
    String line = reader.readLine();
    if (line == null || line.trim().isEmpty()) {
      return 1.0f;
    }
    Float value = null;
    try {
      value = Float.parseFloat(line);
    } catch (NumberFormatException nfe) {
      // do nothing
    }
    return value;
  }
}
