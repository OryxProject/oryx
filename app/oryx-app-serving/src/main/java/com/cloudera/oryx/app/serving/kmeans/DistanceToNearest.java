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

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.cloudera.oryx.api.serving.OryxServingException;
import com.cloudera.oryx.common.text.TextUtils;

/**
 * <p>Responds to a GET request to {@code /distanceToNearest/[datum]}. The inputs is a data point
 * to cluster, delimited, like "1,foo,3.0".</p>
 *
 * <p>The response body contains the distance from the point to its nearest cluster center.
 * The distance function depends on the model. For now the default is Euclidean distance.</p>
 */
@Singleton
@Path("/distanceToNearest")
public final class DistanceToNearest extends AbstractKMeansResource {

  @GET
  @Path("{datum}")
  @Produces({MediaType.TEXT_PLAIN, "text/csv", MediaType.APPLICATION_JSON})
  public String get(@PathParam("datum") String datum) throws OryxServingException {
    check(datum != null && !datum.isEmpty(), "Data is needed to cluster");
    String[] tokens = TextUtils.parseDelimited(datum, ',');
    return cluster(tokens).getSecond().toString();
  }

}
