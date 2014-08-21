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
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.PathSegment;

import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.math.VectorMath;
import com.cloudera.oryx.ml.serving.als.model.ALSServingModel;

/**
 * <p>Responds to a GET request to
 * {@code /estimateForAnonymous/[toItemID]/[itemID1(=value1)](/[itemID2(=value2)]/...)}.
 * That is, 1 or more item IDs are supplied, which may each optionally correspond to
 * a value or else default to 1.</p>
 *
 * <p>Unknown item IDs are ignored, unless all are unknown, in which case a
 * {link HttpServletResponse#SC_BAD_REQUEST} status is returned.</p>
 *
 * <p>Outputs the result of the method call as a value on one line.</p>
 */
@Path("/estimateForAnonymous")
public final class EstimateForAnonymous extends AbstractALSResource {

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("{toItemID}/{itemID : .+}")
  public Double get(@PathParam("toItemID") String toItemID,
                    @PathParam("itemID") List<PathSegment> pathSegmentList) {
    Pair<String[],double[]> itemValuePairs = parseItemValuePairs(pathSegmentList);
    String[] itemIDs = itemValuePairs.getFirst();
    double[] values = itemValuePairs.getSecond();
    ALSServingModel model = getALSServingModel();
    float[] toItemVector = model.getItemVector(toItemID);
    int features = toItemVector.length;
    double[] userItemRowTimesY = new double[features];
    for (int j = 0; j < itemIDs.length; j++) {
      float[] itemVector = model.getItemVector(itemIDs[j]);
      // 0.5 reflects a "don't know" state
      double weight = computeTargetQui(model, values[j], 0.5);
      for (int i = 0; i < features; i++) {
        userItemRowTimesY[i] += weight * itemVector[i];
      }
    }
    double[] anonymousUserFeatures = model.getYTYSolver().solveDToD(userItemRowTimesY);
    return VectorMath.dot(anonymousUserFeatures, toItemVector);
  }

  private static Pair<String[],double[]> parseItemValuePairs(List<PathSegment> pathComponents) {
    int size = pathComponents.size();
    String[] itemIDs = new String[size];
    double[] values = new double[size];
    for (int i = 0; i < size; i++) {
      Pair<String,Double> itemValuePair = parseItemValue(pathComponents.get(i).getPath());
      itemIDs[i] = itemValuePair.getFirst();
      Double value = itemValuePair.getSecond();
      values[i] = value == null ? 1.0 : value;
    }
    return new Pair<>(itemIDs, values);
  }

  private static Pair<String,Double> parseItemValue(String s) {
    int offset = s.indexOf('=');
    if (offset < 0) {
      return new Pair<>(s, null);
    }
    return new Pair<>(s.substring(0, offset), Double.parseDouble(s.substring(offset + 1)));
  }

  /**
   * See also ALSServingModelManager
   */
  private static double computeTargetQui(ALSServingModel model, double value, double currentValue) {
    // We want Qui to change based on value. What's the target value, Qui'?
    // Then we find a new vector Xu' such that Qui' = Xu' * (Yi)^t
    double targetQui;
    if (model.isImplicit()) {
      // Target is really 1, or 0, depending on whether value is positive or negative.
      // This wouldn't account for the strength though. Instead the target is a function
      // of the current value and strength. If the current value is c, and value is positive
      // then the target is somewhere between c and 1 depending on the strength. If current
      // value is already >= 1, there's no effect. Similarly for negative values.
      if (value > 0.0f && currentValue < 1.0) {
        double diff = 1.0 - Math.max(0.0, currentValue);
        targetQui = currentValue + (1.0 - 1.0 / (1.0 + value)) * diff;
      } else if (value < 0.0f && currentValue > 0.0) {
        double diff = -Math.min(1.0, currentValue);
        targetQui = currentValue + (1.0 - 1.0 / (1.0 - value)) * diff;
      } else {
        // No change
        targetQui = Double.NaN;
      }
    } else {
      // Non-implicit -- value is supposed to be the new value
      targetQui = value;
    }
    return targetQui;
  }

}
