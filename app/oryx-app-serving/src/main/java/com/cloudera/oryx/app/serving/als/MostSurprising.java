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

package com.cloudera.oryx.app.serving.als;

import java.util.List;
import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;

import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.collection.PairComparators;
import com.cloudera.oryx.common.math.VectorMath;
import com.cloudera.oryx.app.serving.CSVMessageBodyWriter;
import com.cloudera.oryx.app.serving.IDValue;
import com.cloudera.oryx.app.serving.OryxServingException;
import com.cloudera.oryx.app.serving.als.model.ALSServingModel;

/**
 * <p>Responds to a GET request to {@code /mostSurprising/[userID](?howMany=n)(?offset=o)}.
 *
 * <p>This is like an anti-{@code recommend} method, where the results are taken from among
 * the items that the user has already interacted with, and the results are items that
 * seem least-likely to be interacted with according to the model.
 * Outputs contain item and score pairs, where the score is an opaque
 * value where higher values mean more surprising.</p>
 *
 * <p>If the user, item or user's interacted items are not known to the model, a
 * {@link javax.ws.rs.core.Response.Status#NOT_FOUND} response is generated.</p>
 *
 * <p>{@code howMany} and {@code offset} behavior, and output, are as in {@link Recommend}.</p>
 */
@Singleton
@Path("/mostSurprising")
public final class MostSurprising extends AbstractALSResource {

  @GET
  @Path("{userID}")
  @Produces({MediaType.TEXT_PLAIN, CSVMessageBodyWriter.TEXT_CSV, MediaType.APPLICATION_JSON})
  public List<IDValue> get(
      @PathParam("userID") String userID,
      @DefaultValue("10") @QueryParam("howMany") int howMany,
      @DefaultValue("0") @QueryParam("offset") int offset) throws OryxServingException {

    check(howMany > 0, "howMany must be positive");
    check(offset >= 0, "offset must be nonnegative");

    ALSServingModel model = getALSServingModel();
    float[] userVector = model.getUserVector(userID);
    checkExists(userVector != null, userID);
    List<Pair<String,float[]>> knownItemVectors = model.getKnownItemVectorsForUser(userID);
    checkExists(knownItemVectors != null, userID);

    Iterable<Pair<String,Double>> idDots =
        Iterables.transform(knownItemVectors, new DotsFunction(userVector));

    Ordering<Pair<?,Double>> ordering = Ordering.from(PairComparators.<Double>bySecond());
    return toIDValueResponse(ordering.leastOf(idDots, howMany + offset), howMany, offset);
  }

  private static final class DotsFunction
      implements Function<Pair<String,float[]>,Pair<String,Double>> {
    private final float[] userVector;
    DotsFunction(float[] userVector) {
      this.userVector = userVector;
    }
    @Override
    public Pair<String,Double> apply(Pair<String,float[]> itemIDVector) {
      return new Pair<>(
          itemIDVector.getFirst(),
          VectorMath.dot(userVector, itemIDVector.getSecond()));
    }
  }

}
