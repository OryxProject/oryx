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
import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.PathSegment;

import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.lambda.serving.ModelManagerListener;
import com.cloudera.oryx.lambda.serving.ServingModelManager;
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
public final class EstimateForAnonymous {

  @Context
  private ServletContext servletContext;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("{toItemID}/{itemID : .+}")
  public Double get(@PathParam("toItemID") String toItemID,
                    @PathParam("itemID") List<PathSegment> pathSegmentList) {
    ServingModelManager<?> alsServingModelManager =
        (ServingModelManager<?>) servletContext.getAttribute(ModelManagerListener.MANAGER_KEY);
    ALSServingModel alsServingModel = (ALSServingModel) alsServingModelManager.getModel();
    Pair<String[], float[]> itemValuePairs = parseItemValuePairs(pathSegmentList);
    return alsServingModel.dotProduct(toItemID, itemValuePairs.getFirst(), itemValuePairs.getSecond());
  }

  private static Pair<String[],float[]> parseItemValuePairs(List<PathSegment> pathComponents) {
    List<Pair<String,Float>> itemValuePairs = new ArrayList<>(1);
    for (PathSegment pathComponent : pathComponents) {
      itemValuePairs.add(parseItemValue(pathComponent.getPath()));
    }
    int size = itemValuePairs.size();
    String[] itemIDs = new String[size];
    float[] values = new float[size];
    for (int i = 0; i < size; i++) {
      Pair<String,Float> itemValuePair = itemValuePairs.get(i);
      itemIDs[i] = itemValuePair.getFirst();
      Float value = itemValuePair.getSecond();
      values[i] = value == null ? 1.0f : value;
    }
    return new Pair<>(itemIDs, values);
  }

  private static Pair<String,Float> parseItemValue(String s) {
    if (!s.contains("=")) {
      return new Pair<>(s, null);
    }
    return new Pair<>(s.substring(0, s.indexOf('=')),
        Float.parseFloat(s.substring(s.indexOf('=') + 1)));
  }
}
