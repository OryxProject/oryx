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

import com.cloudera.oryx.ml.serving.als.model.ALSServingModel;
import com.cloudera.oryx.ml.serving.als.model.ALSServingModelManager;

/**
 * <p>Responds to a GET request to {@code /estimate/[userID]/[itemID]} and in turn calls
 * {link ALSServingModel#estimatePreference(int, int[])}.</p>
 * <p/>
 * <p>This REST endpoint can also compute several estimates at once. Send a GET request to
 * {@code /estimate/[userID]/[itemID1](/[itemID2]/...)}. The output are estimates, in the same
 * order as the item ID.</p>
 */
@Path("/estimate")
public final class Estimate {

  @Context
  private ServletContext servletContext;

  @GET
  @Path("{userID}/{itemID : .+}")
  @Produces(MediaType.APPLICATION_JSON)
  public List<Double> getEstimatePreferences(@PathParam("userID") final Integer userID,
                                             @PathParam("itemID") final List<PathSegment> pathSegmentList) {

    ALSServingModelManager alsServingModelManager =
        (ALSServingModelManager) servletContext.getAttribute("ModelManager");
    ALSServingModel alsServingModel = alsServingModelManager.getModel();

    List<Integer> itemIdsList = new ArrayList<>(pathSegmentList.size());
    for (PathSegment pathSegment : pathSegmentList) {
      itemIdsList.add(Integer.valueOf(pathSegment.getPath()));
    }

    return alsServingModel.estimatePreference(userID, itemIdsList);

  }
}
