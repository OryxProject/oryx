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

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.ml.serving.ErrorResponse;
import com.cloudera.oryx.ml.serving.IDValue;
import com.cloudera.oryx.ml.serving.OryxServingException;
import com.cloudera.oryx.ml.serving.als.model.ALSServingModel;

/**
 * <p>Responds to a GET request to
 * {@code /mostSurprising/[userID](?howMany=n)}
 * and in turn calls {link com.cloudera.oryx.ml.serving.als.model.ALSServingModel#mostSurprising(String, int)}.
 * {@code howMany} is the desired number of results to return. If {@code howMany} is not
 * specified, defaults to 10.</p>
 *
 * <p>CSV/JSON output contains one item per line, and each line is of the form {@code itemID, strength},
 * like {@code "I0", 0.53}. Strength is an opaque indicator of the relative surprise of the item.
 * Higher means more surprising</p>
 */
@Path("/mostSurprising")
public final class MostSurprising extends AbstractALSResource {

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response get() {
    return Response.status(Response.Status.BAD_REQUEST).entity(
        new ErrorResponse(Response.Status.BAD_REQUEST, "User ID is required")).build();
  }

  @GET
  @Path("{userID}")
  @Produces({MediaType.APPLICATION_JSON,MediaType.TEXT_PLAIN})
  public List<IDValue> get(@PathParam("userID") String userID,
                           @DefaultValue("10") @QueryParam("howMany") int howMany) throws OryxServingException {
    check(howMany > 0, "howMany must be positive");

    ALSServingModel alsServingModel = getALSServingModel();
    List<Pair<String,Double>> topIDDots =
        alsServingModel.mostSurprising(userID,howMany);
    check(topIDDots != null, Response.Status.NOT_FOUND, userID);

    return Lists.transform(topIDDots,new Function<Pair<String, Double>, IDValue>() {
      @Override
      public IDValue apply(Pair<String, Double> input) {
        return new IDValue(input.getFirst(),input.getSecond());
      }
    });
  }
}
