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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.PathSegment;

import com.koloboke.function.ObjDoubleToDoubleFunction;

import com.cloudera.oryx.api.serving.OryxServingException;
import com.cloudera.oryx.app.als.Rescorer;
import com.cloudera.oryx.app.als.RescorerProvider;
import com.cloudera.oryx.app.serving.IDValue;
import com.cloudera.oryx.app.serving.als.model.ALSServingModel;
import com.cloudera.oryx.common.collection.Pair;

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
 * <p>{@code howMany}, {@code considerKnownItems} and {@code offset} behavior, and output, are as in
 * {@link Recommend}.</p>
 */
@Singleton
@Path("/recommendToAnonymous")
public final class RecommendToAnonymous extends AbstractALSResource {

  @GET
  @Path("{itemID : .+}")
  @Produces({MediaType.TEXT_PLAIN, "text/csv", MediaType.APPLICATION_JSON})
  public List<IDValue> get(
      @PathParam("itemID") List<PathSegment> pathSegments,
      @DefaultValue("10") @QueryParam("howMany") int howMany,
      @DefaultValue("0") @QueryParam("offset") int offset,
      @QueryParam("rescorerParams") List<String> rescorerParams) throws OryxServingException {

    check(!pathSegments.isEmpty(), "Need at least 1 item to make recommendations");
    int howManyOffset = checkHowManyOffset(howMany, offset);

    ALSServingModel model = getALSServingModel();
    List<Pair<String,Double>> parsedPathSegments = EstimateForAnonymous.parsePathSegments(pathSegments);
    float[] anonymousUserFeatures = EstimateForAnonymous.buildTemporaryUserVector(model, parsedPathSegments, null);
    check(anonymousUserFeatures != null, pathSegments.toString());

    List<String> knownItems = parsedPathSegments.stream().map(Pair::getFirst).collect(Collectors.toList());

    Collection<String> knownItemsSet = new HashSet<>(knownItems);
    Predicate<String> allowedFn = v -> !knownItemsSet.contains(v);
    ObjDoubleToDoubleFunction<String> rescoreFn = null;
    RescorerProvider rescorerProvider = getALSServingModel().getRescorerProvider();
    if (rescorerProvider != null) {
      Rescorer rescorer = rescorerProvider.getRecommendToAnonymousRescorer(knownItems,
                                                                           rescorerParams);
      if (rescorer != null) {
        allowedFn = allowedFn.and(id -> !rescorer.isFiltered(id));
        rescoreFn = rescorer::rescore;
      }
    }

    Stream<Pair<String,Double>> topIDDots = model.topN(
        new DotsFunction(anonymousUserFeatures),
        rescoreFn,
        howManyOffset,
        allowedFn);
    return toIDValueResponse(topIDDots, howMany, offset);
  }

}
