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
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.PathSegment;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import com.cloudera.oryx.app.als.ALSUtils;
import com.cloudera.oryx.app.serving.CSVMessageBodyWriter;
import com.cloudera.oryx.app.serving.OryxServingException;
import com.cloudera.oryx.app.serving.als.model.ALSServingModel;
import com.cloudera.oryx.common.collection.Pair;
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
  @Produces({MediaType.TEXT_PLAIN, CSVMessageBodyWriter.TEXT_CSV, MediaType.APPLICATION_JSON})
  public Double get(
      @PathParam("toItemID") String toItemID,
      @PathParam("itemID") List<PathSegment> pathSegments) throws OryxServingException {

    ALSServingModel model = getALSServingModel();
    float[] toItemVector = model.getItemVector(toItemID);
    checkExists(toItemVector != null, toItemID);

    double[] anonymousUserFeatures = buildAnonymousUserFeatures(model, pathSegments);
    return VectorMath.dot(anonymousUserFeatures, toItemVector);
  }

  static double[] buildAnonymousUserFeatures(ALSServingModel model,
                                             List<PathSegment> pathSegments) {
    List<Pair<String,Double>> itemValuePairs = parsePathSegments(pathSegments);
    int features = model.getFeatures();
    double[] userItemRowTimesY = new double[features];
    for (Pair<String,Double> itemValue : itemValuePairs) {
      float[] itemVector = model.getItemVector(itemValue.getFirst());
      if (itemVector != null) {
        // 0.5 reflects a "don't know" state
        double weight = computeTargetQui(model, itemValue.getSecond(), 0.5);
        for (int i = 0; i < features; i++) {
          userItemRowTimesY[i] += weight * itemVector[i];
        }
      }
    }
    return model.getYTYSolver().solveDToD(userItemRowTimesY);
  }

  static List<Pair<String, Double>> parsePathSegments(List<PathSegment> pathSegments) {
    return Lists.transform(pathSegments,
        new Function<PathSegment, Pair<String, Double>>() {
          @Override
          public Pair<String, Double> apply(PathSegment segment) {
            String s = segment.getPath();
            int offset = s.indexOf('=');
            return offset < 0 ?
                new Pair<>(s, 1.0) :
                new Pair<>(s.substring(0, offset),
                    Double.parseDouble(s.substring(offset + 1)));
          }
        });
  }

  /**
   * See also ALSServingModelManager
   */
  private static double computeTargetQui(ALSServingModel model, double value, double currentValue) {
    // We want Qui to change based on value. What's the target value, Qui'?
    // Then we find a new vector Xu' such that Qui' = Xu' * (Yi)^t
    if (model.isImplicit()) {
      return ALSUtils.implicitTargetQui(value, currentValue);
    } else {
      // Non-implicit -- value is supposed to be the new value
      return value;
    }
  }

}
