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
import com.cloudera.oryx.ml.serving.OryxServingException;
import com.cloudera.oryx.ml.serving.als.model.ALSServingModel;

/**
 * <p>Responds to a GET request to {@code /similarityToItem/[toItemID]/[itemID1](/[itemID2]/...)}.
 *
 * <p>This computes cosine similarity between an item and one or more other items.</p>
 *
 * <p>If the first given item is not known to the model, an
 * HTTP 404 Not Found response is generated. For other items, if not known to the model,
 * a 0.0 is returned in the response in that place</p>
 *
 * <p>The output are similarities, in the same order as the item IDs as a JSON array
 * of double values.</p>
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
  public List<Double> get(
      @PathParam("toItemID") String toItemID,
      @PathParam("itemID") List<PathSegment> pathSegmentsList) throws OryxServingException {

    ALSServingModel alsServingModel = getALSServingModel();
    float[] toItemFeatures = alsServingModel.getItemVector(toItemID);
    check(toItemFeatures != null, Response.Status.NOT_FOUND, toItemID);

    double toItemFeaturesNorm = VectorMath.norm(toItemFeatures);
    List<Double> results = new ArrayList<>(pathSegmentsList.size());
    for (PathSegment item : pathSegmentsList) {
      float[] itemFeatures = alsServingModel.getItemVector(item.getPath());
      if (itemFeatures == null) {
        results.add(0.0);
      } else {
        double value = VectorMath.dot(itemFeatures, toItemFeatures) /
            (toItemFeaturesNorm * VectorMath.norm(itemFeatures));
        Preconditions.checkState(!(Double.isInfinite(value) || Double.isNaN(value)), "Bad similarity");
        results.add(value);
      }
    }
    return results;
  }
}
