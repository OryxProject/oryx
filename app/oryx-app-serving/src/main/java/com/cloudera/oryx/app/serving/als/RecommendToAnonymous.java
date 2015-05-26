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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.PathSegment;

import net.openhft.koloboke.function.ObjDoubleToDoubleFunction;
import net.openhft.koloboke.function.Predicate;

import com.cloudera.oryx.app.als.Rescorer;
import com.cloudera.oryx.app.als.RescorerProvider;
import com.cloudera.oryx.common.collection.AndPredicate;
import com.cloudera.oryx.common.collection.NotContainsPredicate;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.app.serving.CSVMessageBodyWriter;
import com.cloudera.oryx.app.serving.IDValue;
import com.cloudera.oryx.app.serving.OryxServingException;
import com.cloudera.oryx.app.serving.als.model.ALSServingModel;

/**
 * <p>Responds to a GET request to
 * {@code /recommendToAnonymous/[itemID1(=value1)](/[itemID2(=value2)]/...)(?howMany=n)(&offset=o)(&rescorerParams=...)}
 * </p>
 *
 * <p>Results are recommended items for an "anonymous" user, along with a score. The user is
 * defined by a set of items and optional interaction strengths, as in
 * {@link EstimateForAnonymous}.
 * Outputs contain item and score pairs, where the score is an opaque
 * value where higher values mean a better recommendation.</p>
 *
 * <p>{@code howMany} and {@code offset} behavior, and output, are as in {@link Recommend}.</p>
 */
@Singleton
@Path("/recommendToAnonymous")
public final class RecommendToAnonymous extends AbstractALSResource {

  @GET
  @Path("{itemID : .+}")
  @Produces({MediaType.TEXT_PLAIN, CSVMessageBodyWriter.TEXT_CSV, MediaType.APPLICATION_JSON})
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

    List<String> knownItems = new ArrayList<>();
    for (Pair<String,?> itemValue : EstimateForAnonymous.parsePathSegments(pathSegments)) {
      knownItems.add(itemValue.getFirst());
    }

    Predicate<String> allowedFn = new NotContainsPredicate<>(new HashSet<>(knownItems));
    ObjDoubleToDoubleFunction<String> rescoreFn = null;
    RescorerProvider rescorerProvider = getALSServingModel().getRescorerProvider();
    if (rescorerProvider != null) {
      Rescorer rescorer = rescorerProvider.getRecommendToAnonymousRescorer(knownItems,
                                                                           rescorerParams);
      if (rescorer != null) {
        allowedFn = new AndPredicate<>(allowedFn, buildRescorerPredicate(rescorer));
        rescoreFn = buildRescoreFn(rescorer);
      }
    }

    List<Pair<String,Double>> topIDDots = model.topN(
        new DotsFunction(anonymousUserFeatures),
        rescoreFn,
        howMany + offset,
        allowedFn);
    return toIDValueResponse(topIDDots, howMany, offset);
  }

}
