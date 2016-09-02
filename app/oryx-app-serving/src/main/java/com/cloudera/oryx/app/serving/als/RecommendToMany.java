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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.function.Predicate;
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
 * {@code /recommendToMany/[userID1](/[userID2]/...)(?howMany=n)(&offset=o)(&considerKnownItems=c)(&rescorerParams=...)}
 * </p>
 *
 * <p>Results are recommended items for the user, along with a score.
 * Outputs contain item and score pairs, where the score is an opaque
 * value where higher values mean a better recommendation.</p>
 *
 * <p>{@code howMany}, {@code considerKnownItems} and {@code offset} behavior, and output,
 * are as in {@link Recommend}.</p>
 */
@Singleton
@Path("/recommendToMany")
public final class RecommendToMany extends AbstractALSResource {

  @GET
  @Path("{userID : .+}")
  @Produces({MediaType.TEXT_PLAIN, "text/csv", MediaType.APPLICATION_JSON})
  public List<IDValue> get(
      @PathParam("userID") List<PathSegment> pathSegmentsList,
      @DefaultValue("10") @QueryParam("howMany") int howMany,
      @DefaultValue("0") @QueryParam("offset") int offset,
      @DefaultValue("false") @QueryParam("considerKnownItems") boolean considerKnownItems,
      @QueryParam("rescorerParams") List<String> rescorerParams) throws OryxServingException {

    check(!pathSegmentsList.isEmpty(), "Need at least 1 user");
    int howManyOffset = checkHowManyOffset(howMany, offset);

    ALSServingModel alsServingModel = getALSServingModel();
    float[][] userFeaturesVectors = new float[pathSegmentsList.size()][];
    Collection<String> userKnownItems = new HashSet<>();

    List<String> userIDs = new ArrayList<>(userFeaturesVectors.length);
    for (int i = 0; i < userFeaturesVectors.length; i++) {
      String userID = pathSegmentsList.get(i).getPath();
      userIDs.add(userID);
      float[] userFeatureVector = alsServingModel.getUserVector(userID);
      checkExists(userFeatureVector != null, userID);
      userFeaturesVectors[i] = userFeatureVector;
      if (!considerKnownItems) {
        userKnownItems.addAll(alsServingModel.getKnownItems(userID));
      }
    }

    Predicate<String> allowedFn = null;
    if (!userKnownItems.isEmpty()) {
      allowedFn = v -> !userKnownItems.contains(v);
    }

    ObjDoubleToDoubleFunction<String> rescoreFn = null;
    RescorerProvider rescorerProvider = getALSServingModel().getRescorerProvider();
    if (rescorerProvider != null) {
      Rescorer rescorer = rescorerProvider.getRecommendRescorer(userIDs, rescorerParams);
      if (rescorer != null) {
        Predicate<String> rescorerPredicate = id -> !rescorer.isFiltered(id);
        allowedFn = allowedFn == null ? rescorerPredicate : allowedFn.and(rescorerPredicate);
        rescoreFn = rescorer::rescore;
      }
    }

    Stream<Pair<String,Double>> topIDDots = alsServingModel.topN(
        new DotsFunction(userFeaturesVectors),
        rescoreFn,
        howManyOffset,
        allowedFn);

    return toIDValueResponse(topIDDots, howMany, offset);
  }

}
