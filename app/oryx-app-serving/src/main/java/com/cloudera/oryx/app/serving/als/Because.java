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

import java.util.Collections;
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
 * <p>Responds to a GET request to {@code /because/[userID]/[itemID](?howMany=n)(&offset=o)}.</p>
 *
 * <p>Results are items that the user has interacted with that best explain why a given
 * item was recommended. Outputs contain item and score pairs, where the score is an opaque
 * value where higher values mean more relevant to recommendation.</p>
 *
 * <p>If the user is not known to the model, a
 * {@link javax.ws.rs.core.Response.Status#NOT_FOUND} response is generated.
 * If the user has no known items associated, the response has no elements.</p>
 *
 * <p>{@code howMany} and {@code offset} behavior, and output, are as in {@link Recommend}.</p>
 */
@Singleton
@Path("/because")
public final class Because extends AbstractALSResource {

  @GET
  @Path("{userID}/{itemID}")
  @Produces({MediaType.TEXT_PLAIN, CSVMessageBodyWriter.TEXT_CSV, MediaType.APPLICATION_JSON})
  public List<IDValue> get(
      @PathParam("userID") String userID,
      @PathParam("itemID") String itemID,
      @DefaultValue("10") @QueryParam("howMany") int howMany,
      @DefaultValue("0") @QueryParam("offset") int offset) throws OryxServingException {

    check(howMany > 0, "howMany must be positive");
    check(offset >= 0, "offset must be non-negative");

    ALSServingModel model = getALSServingModel();
    float[] itemVector = model.getItemVector(itemID);
    checkExists(itemVector != null, itemID);
    List<Pair<String,float[]>> knownItemVectors = model.getKnownItemVectorsForUser(userID);
    if (knownItemVectors == null || knownItemVectors.isEmpty()) {
      return Collections.emptyList();
    }

    Iterable<Pair<String,Double>> idSimilarities =
        Iterables.transform(knownItemVectors, new CosineSimilarityFunction(itemVector));

    Ordering<Pair<?,Double>> ordering =
        Ordering.from(PairComparators.<Double>bySecond());
    return toIDValueResponse(
        ordering.greatestOf(idSimilarities, howMany + offset), howMany, offset);
  }

  private static final class CosineSimilarityFunction
      implements Function<Pair<String,float[]>,Pair<String,Double>> {
    private final float[] itemVector;
    private final double itemVectorNorm;
    CosineSimilarityFunction(float[] itemVector) {
      this.itemVector = itemVector;
      this.itemVectorNorm = VectorMath.norm(itemVector);
    }
    @Override
    public Pair<String,Double> apply(Pair<String,float[]> itemIDVector) {
      float[] otherItemVector = itemIDVector.getSecond();
      double cosineSimilarity =  VectorMath.dot(itemVector, otherItemVector) /
          (itemVectorNorm * VectorMath.norm(otherItemVector));
      return new Pair<>(itemIDVector.getFirst(), cosineSimilarity);
    }
  }

}
