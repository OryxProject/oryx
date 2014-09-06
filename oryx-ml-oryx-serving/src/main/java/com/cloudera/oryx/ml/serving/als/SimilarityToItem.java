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
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;

import com.google.common.base.Preconditions;

import com.cloudera.oryx.common.math.VectorMath;
import com.cloudera.oryx.ml.serving.ErrorResponse;
import com.cloudera.oryx.ml.serving.als.model.ALSServingModel;

/**
 * <p>This REST endpoint can also compute several similarities at once. Send a GET request to
 * {@code /similarityToItem/[toItemID]/[itemID1](/[itemID2]/...)}. The output are similarities, in the same
 * order as the item ID.</p>
 */
@Path("/similarityToItem")
public final class SimilarityToItem extends AbstractALSResource {

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response get() {
    return Response.status(Response.Status.BAD_REQUEST).entity(
        new ErrorResponse(Response.Status.BAD_REQUEST, "toItemID is required")).build();
  }

  @GET
  @Path("{toItemID}/{itemID : .+}")
  @Produces(MediaType.APPLICATION_JSON)
  public List<Double> get(@PathParam("toItemID") String toItemID,
                          @PathParam("itemID") List<PathSegment> pathSegmentsList) {
    ALSServingModel alsServingModel = getALSServingModel();
    float[] toItemFeatures = alsServingModel.getItemVector(toItemID);
    double toItemFeaturesNorm = VectorMath.norm(toItemFeatures);
    List<Double> results = new ArrayList<>(pathSegmentsList.size());
    for (PathSegment item : pathSegmentsList) {
      float[] itemFeatures = alsServingModel.getItemVector(item.getPath());
      if (itemFeatures == null) {
        results.add(0.0);
      } else {
        double itemFeaturesNorm = VectorMath.norm(itemFeatures);
        double value = VectorMath.dot(itemFeatures, toItemFeatures)/(toItemFeaturesNorm * itemFeaturesNorm);
        Preconditions.checkState(!(Double.isInfinite(value) || Double.isNaN(value)), "Bad similarity");
        results.add(value);
      }
    }
    return results;
  }
}
