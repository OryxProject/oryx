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

package com.cloudera.oryx.app.serving.rdf;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.cloudera.oryx.app.serving.CSVMessageBodyWriter;
import com.cloudera.oryx.app.serving.OryxServingException;

/**
 * <p>Responds to a GET request to {@code /classify/[datum]}. The input is one data point to classify,
 * delimited, like "1,foo,3.0". The response body contains the result of classification on one line.
 * The result depends on the classifier or regressor --  could be a number or a category name.</p>
 */
@Path("/classify")
public final class Classify extends AbstractRDFResource {

  @GET
  @Path("{datum}")
  @Produces({MediaType.TEXT_PLAIN, CSVMessageBodyWriter.TEXT_CSV, MediaType.APPLICATION_JSON})
  public Response get(@PathParam("datum") String datum) throws OryxServingException {

    check(datum != null && !datum.isEmpty(), "Input Data needed to classify");

    return Response.ok().build();
  }

}
