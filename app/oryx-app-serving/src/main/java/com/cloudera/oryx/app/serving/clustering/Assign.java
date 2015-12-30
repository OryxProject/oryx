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

package com.cloudera.oryx.app.serving.clustering;

import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.Part;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import com.cloudera.oryx.api.serving.OryxServingException;
import com.cloudera.oryx.app.serving.AbstractOryxResource;
import com.cloudera.oryx.app.serving.clustering.model.ClusteringServingModel;
import com.cloudera.oryx.common.text.TextUtils;

/**
 * <p>Responds to a GET request to {@code /assign/[datum]}, or a POST to {@code /assign}
 * containing several data points, one on each line. The inputs are data points to cluster,
 * delimited, like "1,foo,3.0".</p>
 *
 * <p>The response body contains the result of clustering -- the IDs of the assigned clusters --
 * one for each input data point, one per line.</p>
 */
@Singleton
@Path("/assign")
public final class Assign extends AbstractOryxResource {

  @GET
  @Path("{datum}")
  @Produces({MediaType.TEXT_PLAIN, "text/csv", MediaType.APPLICATION_JSON})
  public String get(@PathParam("datum") String datum) throws OryxServingException {
    return nearestClusterID(datum);
  }

  @POST
  @Consumes({MediaType.TEXT_PLAIN, "text/csv", MediaType.APPLICATION_JSON})
  @Produces({MediaType.TEXT_PLAIN, "text/csv", MediaType.APPLICATION_JSON})
  public List<String> post(Reader reader) throws IOException, OryxServingException {
    return doPost(maybeBuffer(reader));
  }

  @POST
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces({MediaType.TEXT_PLAIN, "text/csv", MediaType.APPLICATION_JSON})
  public List<String> post(@Context HttpServletRequest request)
      throws IOException, OryxServingException {
    List<String> result = new ArrayList<>();
    for (Part item : parseMultipart(request)) {
      try (BufferedReader reader = maybeBuffer(maybeDecompress(item))) {
        result.addAll(doPost(reader));
      }
    }
    return result;
  }

  private List<String> doPost(BufferedReader buffered) throws IOException, OryxServingException {
    List<String> predictions = new ArrayList<>();
    for (String line; (line = buffered.readLine()) != null;) {
      predictions.add(nearestClusterID(line));
    }
    return predictions;
  }

  private String nearestClusterID(String datum) throws OryxServingException {
    check(datum != null && !datum.isEmpty(), "Data is needed to cluster");
    String[] tokens = TextUtils.parseDelimited(datum, ',');
    ClusteringServingModel model = (ClusteringServingModel) getServingModel();
    int nearestID;
    try {
      nearestID = model.nearestClusterID(tokens);
    } catch (IllegalArgumentException iae) {
      throw new OryxServingException(Response.Status.BAD_REQUEST, iae.getMessage());
    }
    return Integer.toString(nearestID);
  }

}
