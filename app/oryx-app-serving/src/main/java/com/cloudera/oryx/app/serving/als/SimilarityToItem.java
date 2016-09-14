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

import com.google.common.base.Preconditions;

import com.cloudera.oryx.api.serving.OryxServingException;
import com.cloudera.oryx.app.serving.als.model.ALSServingModel;
import com.cloudera.oryx.common.math.VectorMath;

/**
 * <p>Responds to a GET request to {@code /similarityToItem/[toItemID]/[itemID1](/[itemID2]/...)}.
 *
 * <p>This computes cosine similarity between an item and one or more other items.</p>
 *
 * <p>The output are similarities, in the same order as the item IDs, with format
 * as in {@link Estimate}</p>
 */
@Singleton
@Path("/similarityToItem")
public final class SimilarityToItem extends AbstractALSResource {

  @GET
  @Path("{toItemID}/{itemID : .+}")
  @Produces({MediaType.TEXT_PLAIN, "text/csv", MediaType.APPLICATION_JSON})
  public List<Double> get(
      @PathParam("toItemID") String toItemID,
      @PathParam("itemID") List<PathSegment> pathSegmentsList) throws OryxServingException {

    ALSServingModel alsServingModel = getALSServingModel();
    float[] toItemFeatures = alsServingModel.getItemVector(toItemID);
    checkExists(toItemFeatures != null, toItemID);

    double toItemFeaturesNorm = VectorMath.norm(toItemFeatures);
    return pathSegmentsList.stream().map(item -> {
      float[] itemFeatures = alsServingModel.getItemVector(item.getPath());
      if (itemFeatures == null) {
        return 0.0;
      } else {
        double value = VectorMath.cosineSimilarity(itemFeatures, toItemFeatures, toItemFeaturesNorm);
        Preconditions.checkState(!(Double.isInfinite(value) || Double.isNaN(value)), "Bad similarity");
        return value;
      }
    }).collect(Collectors.toList());
  }
}
