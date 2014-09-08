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

import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;

import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.ml.serving.ErrorResponse;
import com.cloudera.oryx.ml.serving.IDValue;
import com.cloudera.oryx.ml.serving.OryxServingException;
import com.cloudera.oryx.ml.serving.als.model.ALSServingModel;

/**
 * <p>Responds to a GET request to {@code /similarity/[itemID1](/[itemID2]/...)(?howMany=n)(&offset=o)(&rescorerParams=...)},
 * and in turn calls {link ALSServingModel#mostSimilarItems(String[], int)} with the supplied values.
 * {@code offset} is an offset into the entire list of results; {@code howMany} is the desired
 * number of results to return from there. For example, {@code offset=30} and {@code howMany=5}
 * will cause the implementation to retrieve 35 results internally and output the last 5.
 * If {@code howMany} is not specified, defaults to 10.
 * {@code offset} defaults to 0.</p>
 *
 * <p>Unknown item IDs are ignored, unless all are unknown, in which case a
 * {link HttpServletResponse#SC_BAD_REQUEST} status is returned.</p>
 *
 * <p>Outputs item/score pairs like {@link Recommend} does.</p>
 *
 * <p>This does something slightly different from
 * {@link RecommendToAnonymous}; see
 * {link OryxRecommender#mostSimilarItems(String, int)}.</p>
 */
@Path("/similarity")
public final class Similarity extends AbstractALSResource {

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response get() {
    return Response.status(Response.Status.BAD_REQUEST).entity(
        new ErrorResponse(Response.Status.BAD_REQUEST, "Atleast 1 itemID is required")).build();
  }

  @GET
  @Path("{itemID : .+}")
  @Produces(MediaType.APPLICATION_JSON)
  public List<IDValue> get(
      @PathParam("itemID") List<PathSegment> pathSegmentsList,
      @DefaultValue("10") @QueryParam("howMany") int howMany,
      @DefaultValue("0") @QueryParam("offset") int offset,
      @QueryParam("rescorerParams") List<String> rescorerParams) throws OryxServingException {

    check(howMany > 0, "howMany must be positive");
    check(offset >= 0, "offset must be non-negative");
    check(pathSegmentsList.size() > 0, "Need atleast 1 item to determine similarity");

    ALSServingModel alsServingModel = getALSServingModel();
    List<String> itemsList = new ArrayList<>(pathSegmentsList.size());
    for (PathSegment pathSegment : pathSegmentsList) {
      itemsList.add(pathSegment.getPath());
    }

    List<Pair<String,Double>> topSimilarities =
        alsServingModel.mostSimilarItems(itemsList, howMany + offset);
    check(topSimilarities != null, Response.Status.NOT_FOUND, "No similar items found");

    return toIDValueResponse(topSimilarities, howMany, offset);
  }
}
