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
import java.io.Reader;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.cloudera.oryx.ml.serving.CSVMessageBodyWriter;
import com.cloudera.oryx.ml.serving.OryxServingException;

/**
 * <p>Responds to a POST request to {@code /pref/[userID]/[itemID]}. The first line of the request
 * body is parsed as a strength score for the user-item preference. If the request body is empty,
 * the value is 1.0.</p>
 *
 * <p>Also responds to a DELETE request to the same path, which will signal the removal
 * of a user-item association.</p>
 */
@Path("/pref")
public final class Preference extends AbstractALSResource {

  @POST
  @Path("{userID}/{itemID}")
  @Consumes({CSVMessageBodyWriter.TEXT_CSV, MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  public void post(
      @PathParam("userID") String userID,
      @PathParam("itemID") String itemID,
      Reader reader) throws IOException, OryxServingException {
    BufferedReader bufferedReader =
        reader instanceof BufferedReader ? (BufferedReader) reader : new BufferedReader(reader);
    String line = bufferedReader.readLine();
    String value = validateAndStandardizeStrength(line);
    sendToQueue(userID, itemID, value);
  }

  @DELETE
  @Path("{userID}/{itemID}")
  public void delete(
      @PathParam("userID") String userID,
      @PathParam("itemID") String itemID) {
    sendToQueue(userID, itemID, "");
  }

  private void sendToQueue(String userID, String itemID, String value) {
    getInputProducer().send(userID + "," + itemID + "," + value + "," + System.currentTimeMillis());
  }

  static String validateAndStandardizeStrength(String raw) throws OryxServingException {
    if (raw == null || raw.trim().isEmpty()) {
      return "1";
    }
    float value;
    try {
      value = Float.parseFloat(raw);
    } catch (NumberFormatException nfe) {
      throw new OryxServingException(Response.Status.BAD_REQUEST, nfe.getMessage());
    }
    check(!Float.isNaN(value) && !Float.isInfinite(value), raw);
    return Float.toString(value);
  }

}
