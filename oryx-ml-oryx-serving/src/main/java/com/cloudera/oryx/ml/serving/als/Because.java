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

import java.util.List;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.ml.serving.ErrorResponse;
import com.cloudera.oryx.ml.serving.IDValue;
import com.cloudera.oryx.ml.serving.OryxServingException;
import com.cloudera.oryx.ml.serving.als.model.ALSServingModel;

/**
 * <p>Responds to a GET request to {@code /because/[userID]/[itemID](?howMany=n)(&offset=o)}.</p>
 *
 * <p>Results are items that the user has interacted with that best explain why a given
 * item was recommended. Outputs contain item and score pairs, where the score is an opaque
 * value where higher values mean more relevant to recommendation.</p>
 *
 * <p>If the user, item or user's interacted items are not known to the model, an
 * HTTP 404 Not Found response is generated.</p>
 *
 * <p>{@code howMany} and {@code offset} behavior, and output, are as in {@link Recommend}.</p>
 */
@Path("/because")
public final class Because extends AbstractALSResource {

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response get() {
    return Response.status(Response.Status.BAD_REQUEST).entity(
        new ErrorResponse(Response.Status.BAD_REQUEST, "User ID and item ID are required")).build();
  }

  @GET
  @Path("{userID}/{itemID}")
  @Produces(MediaType.APPLICATION_JSON)
  public List<IDValue> get(
      @PathParam("userID") String userID,
      @PathParam("itemID") String itemID,
      @DefaultValue("10") @QueryParam("howMany") int howMany,
      @DefaultValue("0") @QueryParam("offset") int offset) throws OryxServingException {

    check(howMany > 0, "howMany must be positive");
    check(offset >= 0, "offset must be non-negative");

    ALSServingModel alsServingModel = getALSServingModel();
    List<Pair<String,Double>> topCosineSimilar =
        alsServingModel.topCosineSimilarityWithItemVector(userID, itemID, howMany + offset);
    check(topCosineSimilar != null, Response.Status.NOT_FOUND, userID);

    return toIDValueResponse(topCosineSimilar, howMany, offset);
  }
}
