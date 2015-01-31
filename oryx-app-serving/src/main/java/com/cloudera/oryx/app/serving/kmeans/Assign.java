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

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.fileupload.FileItem;

import com.cloudera.oryx.app.serving.CSVMessageBodyWriter;
import com.cloudera.oryx.app.serving.OryxServingException;
import com.cloudera.oryx.common.text.TextUtils;

/**
 * <p>Responds to a GET request to {@code /assign/[datum]}, or a POST to {@code /assign}
 * containing several data points, one on each line. The inputs are data points to cluster,
 * delimited, like "1,foo,3.0".</p>
 *
 * <p>The response body contains the result of clustering, one for each input data point, one per
 * line.</p>
 */
@Path("/assign")
public final class Assign extends AbstractKMeansResource {

  @GET
  @Path("{datum}")
  @Produces({MediaType.TEXT_PLAIN, CSVMessageBodyWriter.TEXT_CSV, MediaType.APPLICATION_JSON})
  public String get(@PathParam("datum") String datum) throws OryxServingException {
    return nearestClusterID(datum).toString();
  }

  @POST
  @Consumes({MediaType.TEXT_PLAIN, CSVMessageBodyWriter.TEXT_CSV, MediaType.APPLICATION_JSON})
  @Produces({MediaType.TEXT_PLAIN, CSVMessageBodyWriter.TEXT_CSV, MediaType.APPLICATION_JSON})
  public List<String> post(Reader reader) throws IOException, OryxServingException {
    return doPost(maybeBuffer(reader));
  }

  @POST
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces({MediaType.TEXT_PLAIN, CSVMessageBodyWriter.TEXT_CSV, MediaType.APPLICATION_JSON})
  public List<String> post(@Context HttpServletRequest request)
      throws IOException, OryxServingException {
    List<String> result = new ArrayList<>();
    for (FileItem item : parseMultipart(request)) {
      InputStream in = maybeDecompress(item.getContentType(), item.getInputStream());
      try (BufferedReader reader = maybeBuffer(new InputStreamReader(in, StandardCharsets.UTF_8))) {
        result.addAll(doPost(reader));
      }
    }
    return result;
  }

  private List<String> doPost(BufferedReader buffered) throws IOException, OryxServingException {
    List<String> predictions = new ArrayList<>();
    String line;
    while ((line = buffered.readLine()) != null) {
      predictions.add(nearestClusterID(line).toString());
    }
    return predictions;
  }

  private Integer nearestClusterID(String datum) throws OryxServingException {
    check(datum != null && !datum.isEmpty(), "Data is needed to cluster");
    String[] tokens = TextUtils.parseCSV(datum);
    return cluster(tokens).getFirst();
  }

}
