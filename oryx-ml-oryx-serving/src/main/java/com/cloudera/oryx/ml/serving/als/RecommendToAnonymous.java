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
import javax.ws.rs.core.PathSegment;

import com.carrotsearch.hppc.ObjectOpenHashSet;
import com.carrotsearch.hppc.ObjectSet;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.collect.Iterables;

import com.cloudera.oryx.common.ClosedFunction;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.ml.serving.CSVMessageBodyWriter;
import com.cloudera.oryx.ml.serving.IDValue;
import com.cloudera.oryx.ml.serving.OryxServingException;
import com.cloudera.oryx.ml.serving.als.model.ALSServingModel;

/**
 * <p>Responds to a GET request to
 * {@code /recommendToAnonymous/[itemID1(=value1)](/[itemID2(=value2)]/...)(?howMany=n)(&offset=o)}.</p>
 *
 * <p>Results are recommended items for an "anonymous" user, along with a score. The user is
 * defined by a set of items and optional interaction strengths, as in
 * {@link com.cloudera.oryx.ml.serving.als.EstimateForAnonymous}.
 * Outputs contain item and score pairs, where the score is an opaque
 * value where higher values mean a better recommendation.</p>
 *
 * <p>{@code howMany} and {@code offset} behavior, and output, are as in {@link Recommend}.</p>
 */
@Path("/recommendToAnonymous")
public final class RecommendToAnonymous extends AbstractALSResource {

  @GET
  @Path("{itemID : .+}")
  @Produces({CSVMessageBodyWriter.TEXT_CSV, MediaType.APPLICATION_JSON})
  public List<IDValue> get(
      @PathParam("itemID") List<PathSegment> pathSegments,
      @DefaultValue("10") @QueryParam("howMany") int howMany,
      @DefaultValue("0") @QueryParam("offset") int offset,
      @QueryParam("rescorerParams") List<String> rescorerParams) throws OryxServingException {

    check(howMany > 0, "howMany must be positive");
    check(offset >= 0, "offset must be nonnegative");

    ALSServingModel model = getALSServingModel();
    double[] anonymousUserFeatures =
        EstimateForAnonymous.buildAnonymousUserFeatures(model, pathSegments);

    final ObjectSet<String> knownItems = new ObjectOpenHashSet<>();
    for (Pair<String,?> itemValue : EstimateForAnonymous.parsePathSegments(pathSegments)) {
      knownItems.add(itemValue.getFirst());
    }

    List<Pair<String,Double>> topIDDots = model.topN(
        new ClosedFunction<Iterable<ObjectObjectCursor<String,float[]>>>() {
          @Override
          public Iterable<ObjectObjectCursor<String,float[]>> apply(Iterable<ObjectObjectCursor<String,float[]>> input) {
            return Iterables.filter(input, new NotKnownPredicate(knownItems));
          }
        },
        new DotsFunction(anonymousUserFeatures),
        howMany + offset);
    return toIDValueResponse(topIDDots, howMany, offset);
  }

}
