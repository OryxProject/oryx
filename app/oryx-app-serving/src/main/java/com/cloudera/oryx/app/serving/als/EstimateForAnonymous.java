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
import java.util.stream.Collectors;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;

import com.cloudera.oryx.api.serving.OryxServingException;
import com.cloudera.oryx.app.als.ALSUtils;
import com.cloudera.oryx.app.serving.als.model.ALSServingModel;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.math.Solver;
import com.cloudera.oryx.common.math.VectorMath;

/**
 * <p>Responds to a GET request to
 * {@code /estimateForAnonymous/[toItemID]/[itemID1(=value1)](/[itemID2(=value2)]/...)}.
 * That is, 1 or more item IDs are supplied, which may each optionally correspond to
 * a value or else default to 1.</p>
 *
 * <p>Unknown item IDs are ignored.</p>
 *
 * <p>Outputs the result of the method call as a value on one line.</p>
 */
@Singleton
@Path("/estimateForAnonymous")
public final class EstimateForAnonymous extends AbstractALSResource {

  @GET
  @Path("{toItemID}/{itemID : .+}")
  @Produces({MediaType.TEXT_PLAIN, "text/csv", MediaType.APPLICATION_JSON})
  public Double get(
      @PathParam("toItemID") String toItemID,
      @PathParam("itemID") List<PathSegment> pathSegments) throws OryxServingException {

    ALSServingModel model = getALSServingModel();
    float[] toItemVector = model.getItemVector(toItemID);
    checkExists(toItemVector != null, toItemID);

    float[] anonymousUserFeatures = buildTemporaryUserVector(model, parsePathSegments(pathSegments), null);
    return anonymousUserFeatures == null ? 0.0 : VectorMath.dot(anonymousUserFeatures, toItemVector);
  }

  /**
   * Builds a temporary user feature vector given an (optional) existing user vector and a context
   * of items.
   *
   * @param model current ALS model
   * @param parsedPathSegments request path segments containing item-value pairs
   * @param Xu existing feature vector for the user (may be {@code null})
   * @return user feature vector
   */
  static float[] buildTemporaryUserVector(ALSServingModel model,
                                          List<Pair<String,Double>> parsedPathSegments,
                                          float[] Xu) throws OryxServingException {
    boolean implicit = model.isImplicit();
    Solver solver = model.getYTYSolver();
    check(solver != null, Response.Status.SERVICE_UNAVAILABLE, "No solver available for model yet");
    for (Pair<String,Double> itemValue : parsedPathSegments) {
      float[] Yi = model.getItemVector(itemValue.getFirst());
      // Given value is taken to be the fictitious current value of Qui = Xu * Yi^T
      float[] newXu = ALSUtils.computeUpdatedXu(solver, itemValue.getSecond(), Xu, Yi, implicit);
      if (newXu != null) {
        Xu = newXu;
      }
    }
    return Xu;
  }

  static List<Pair<String, Double>> parsePathSegments(List<PathSegment> pathSegments) {
    return pathSegments.stream().map(segment -> {
      String s = segment.getPath();
      int offset = s.indexOf('=');
      return offset < 0 ?
          new Pair<>(s, 1.0) :
          new Pair<>(s.substring(0, offset), Double.parseDouble(s.substring(offset + 1)));
    }).collect(Collectors.toList());
  }

}
