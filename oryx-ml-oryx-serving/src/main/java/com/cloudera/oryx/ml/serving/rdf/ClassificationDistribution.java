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

package com.cloudera.oryx.ml.serving.rdf;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.cloudera.oryx.ml.serving.CSVMessageBodyWriter;
import com.cloudera.oryx.ml.serving.OryxServingException;

/**
 * <p>Responds to a GET request to {@code /classificationDistribution/[datum]}.
 * Like {@link Classify} but this returns not just the most probable category,
 * but all categories and their associated probability. In the case of regression,
 * this may or may not return multiple values.
 * The output is "category,probability", one per line for each value.</p>
 */
@Path("/classificationDistribution")
public final class ClassificationDistribution extends AbstractRDFResource {

  @GET
  @Path("{datum}")
  @Produces({CSVMessageBodyWriter.TEXT_CSV, MediaType.APPLICATION_JSON})
  public Response get(@PathParam("datum") String datum) throws OryxServingException {

    check(datum != null && !datum.isEmpty(), "Input Data needed to classify");

    return Response.ok().build();
  }

}
