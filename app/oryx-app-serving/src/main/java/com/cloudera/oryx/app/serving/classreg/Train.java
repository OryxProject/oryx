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

package com.cloudera.oryx.app.serving.classreg;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.Part;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import com.cloudera.oryx.api.serving.OryxServingException;
import com.cloudera.oryx.app.serving.AbstractOryxResource;

/**
 * <p>Responds to POST request to {@code /train}. The input is one or more data points
 * to train, one for each line of the request body. Each data point is a delimited line of input like
 * "1,foo,3.0". Also, one data point can be supplied by POSTing to {@code /train/[datum]}.
 * The classifier updates to learn in some way from the new data. The response is empty.</p>
 */
@Singleton
@Path("/train")
public final class Train extends AbstractOryxResource {

  @POST
  @Consumes({MediaType.TEXT_PLAIN, "text/csv", MediaType.APPLICATION_JSON})
  public void post(Reader reader) throws IOException, OryxServingException {
    checkNotReadOnly();
    doPost(maybeBuffer(reader));
  }

  @POST
  @Path("{datum}")
  public void post(@PathParam("datum") String datum) throws OryxServingException {
    checkNotReadOnly();
    sendInput(datum);
  }

  @POST
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  public void post(@Context HttpServletRequest request) throws IOException, OryxServingException {
    checkNotReadOnly();
    for (Part item : parseMultipart(request)) {
      try (BufferedReader reader = maybeBuffer(maybeDecompress(item))) {
        doPost(reader);
      }
    }
  }

  private void doPost(BufferedReader buffered) throws IOException {
    for (String line; (line = buffered.readLine()) != null;) {
      sendInput(line);
    }
  }

}
