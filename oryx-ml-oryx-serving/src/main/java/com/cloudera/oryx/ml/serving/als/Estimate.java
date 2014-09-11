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

import com.google.common.base.Preconditions;

import com.cloudera.oryx.common.math.VectorMath;
import com.cloudera.oryx.ml.serving.CSVMessageBodyWriter;
import com.cloudera.oryx.ml.serving.OryxServingException;
import com.cloudera.oryx.ml.serving.als.model.ALSServingModel;

/**
 * <p>Responds to a GET request to {@code /estimate/[userID]/[itemID]}.</p>
 *
 * <p>The results are opaque values which estimate the strength of interaction between a user
 * and item. Higher values mean stronger interaction.</p>
 *
 * <p>This REST endpoint can also compute several estimates at once. Send a GET request to
 * {@code /estimate/[userID]/[itemID1](/[itemID2]/...)}.</p>
 *
 * <p>If the user is not known to the model, a {@link javax.ws.rs.core.Response.Status#NOT_FOUND}
 * response is generated. If any item is not known, then that entry in the response will be 0.</p>
 *
 * <p>The output are estimates, in the same order as the item IDs. For default CSV output,
 * each line contains one estimate. For JSON output, the result is an array of estimates.</p>
 */
@Path("/estimate")
public final class Estimate extends AbstractALSResource {

  @GET
  @Path("{userID}/{itemID : .+}")
  @Produces({CSVMessageBodyWriter.TEXT_CSV, MediaType.APPLICATION_JSON})
  public List<Double> get(@PathParam("userID") String userID,
                          @PathParam("itemID") List<PathSegment> pathSegmentsList)
      throws OryxServingException {
    ALSServingModel model = getALSServingModel();
    float[] userFeatures = model.getUserVector(userID);
    checkExists(userFeatures != null, userID);
    List<Double> results = new ArrayList<>(pathSegmentsList.size());
    for (PathSegment pathSegment : pathSegmentsList) {
      float[] itemFeatures = model.getItemVector(pathSegment.getPath());
      if (itemFeatures == null) {
        results.add(0.0);
      } else {
        double value = VectorMath.dot(itemFeatures, userFeatures);
        Preconditions.checkState(!(Double.isInfinite(value) || Double.isNaN(value)), "Bad estimate");
        results.add(value);
      }
    }
    return results;
  }
}
