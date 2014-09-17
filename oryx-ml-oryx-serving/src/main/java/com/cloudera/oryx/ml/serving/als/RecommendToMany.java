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

import com.carrotsearch.hppc.ObjectOpenHashSet;
import com.carrotsearch.hppc.ObjectSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;

import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.math.VectorMath;
import com.cloudera.oryx.ml.serving.CSVMessageBodyWriter;
import com.cloudera.oryx.ml.serving.IDValue;
import com.cloudera.oryx.ml.serving.OryxServingException;
import com.cloudera.oryx.ml.serving.als.model.ALSServingModel;

/**
 * <p>Responds to a GET request to
 * {@code /recommendToMany/[userID1](/[userID2]/...)(?howMany=n)(&offset=o)(&considerKnownItems=c)}.</p>
 *
 * <p>Results are recommended items for the user, along with a score.
 * Outputs contain item and score pairs, where the score is an opaque
 * value where higher values mean a better recommendation.</p>
 *
 * <p>{@code howMany}, {@code considerKnownItems} and {@code offset} behavior, and output,
 * are as in {@link Recommend}.</p>
 */
@Path("/recommendToMany")
public final class RecommendToMany extends AbstractALSResource {

  @GET
  @Path("{userID : .+}")
  @Produces({CSVMessageBodyWriter.TEXT_CSV, MediaType.APPLICATION_JSON})
  public List<IDValue> get(
      @PathParam("userID") List<PathSegment> pathSegmentsList,
      @DefaultValue("10") @QueryParam("howMany") int howMany,
      @DefaultValue("0") @QueryParam("offset") int offset,
      @DefaultValue("false") @QueryParam("considerKnownItems") boolean considerKnownItems,
      @QueryParam("rescorerParams") List<String> rescorerParams) throws OryxServingException {

    check(!pathSegmentsList.isEmpty() && pathSegmentsList.size() > 0, "Need atleast 1 user");
    check(howMany > 0, "howMany must be positive");
    check(offset >= 0, "offset must be non-negative");

    ALSServingModel alsServingModel = getALSServingModel();
    List<double[]> userFeaturesList = new ArrayList<>(pathSegmentsList.size());
    ObjectSet<String> userKnownItems = new ObjectOpenHashSet<>();

    for (PathSegment pathSegment : pathSegmentsList) {
      String userID = pathSegment.getPath();
      float[] userFeatureVector = alsServingModel.getUserVector(userID);
      if (userFeatureVector != null) {
        userFeaturesList.add(VectorMath.toDoubles(userFeatureVector));
      }
      if (!considerKnownItems) {
        ObjectSet<String> knownItems = alsServingModel.getKnownItems(userID);
        if (knownItems != null && !knownItems.isEmpty()) {
          synchronized (knownItems) {
            for (ObjectCursor<String> knownItem : knownItems) {
              userKnownItems.add(knownItem.value);
            }
          }
        }
      }
    }

    List<Pair<String,Double>> topIDDots = alsServingModel.topN(
        new DotsFunction(userFeaturesList),
        howMany + offset,
        new NotKnownPredicate(userKnownItems));

    return toIDValueResponse(topIDDots, howMany, offset);
  }

}
