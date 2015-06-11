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

package com.cloudera.oryx.app.serving.kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.apache.commons.fileupload.FileItem;

import com.cloudera.oryx.app.serving.CSVMessageBodyWriter;
import com.cloudera.oryx.app.serving.OryxServingException;

/**
 * <p>Responds to POST request to {@code /add}. The input is one or more data points
 * to add to the clustering, one for each line of the request body. Each data point is a
 * delimited line of input like "1,-4,3.0". Also, one data point can be supplied by
 * POSTing to {@code /add/[datum]}. The clusters update to learn in some way from the new data.
 * The response is empty.</p>
 */
@Singleton
@Path("/add")
public final class Add extends AbstractKMeansResource {

  @POST
  @Consumes({MediaType.TEXT_PLAIN, CSVMessageBodyWriter.TEXT_CSV, MediaType.APPLICATION_JSON})
  public void post(Reader reader) throws IOException {
    doPost(maybeBuffer(reader));
  }

  @POST
  @Path("{datum}")
  public void post(@PathParam("datum") String datum) {
    sendInput(datum);
  }

  @POST
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  public void post(@Context HttpServletRequest request) throws IOException, OryxServingException {
    for (FileItem item : parseMultipart(request)) {
      InputStream in = maybeDecompress(item.getContentType(), item.getInputStream());
      try (BufferedReader reader = maybeBuffer(new InputStreamReader(in, StandardCharsets.UTF_8))) {
        doPost(reader);
      }
    }
  }

  private void doPost(BufferedReader buffered) throws IOException {
    String line;
    while ((line = buffered.readLine()) != null) {
      sendInput(line);
    }
  }
}
