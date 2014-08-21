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

import com.google.common.primitives.Doubles;

/**
 * <p>Responds to a GET request to {@code /estimate/[userID]/[itemID]} and in turn calls
 * {@link com.cloudera.oryx.ml.serving.als.model.ALSServingModel#dotProduct(String,String[])}.</p>
 *
 * <p>This REST endpoint can also compute several estimates at once. Send a GET request to
 * {@code /estimate/[userID]/[itemID1](/[itemID2]/...)}. The output are estimates, in the same
 * order as the item ID.</p>
 */
@Path("/estimate")
public final class Estimate extends AbstractALSResource {

  @GET
  @Path("{userID}/{itemID : .+}")
  @Produces(MediaType.APPLICATION_JSON)
  public List<Double> get(@PathParam("userID") String userID,
                          @PathParam("itemID") List<PathSegment> pathSegmentsList) {
    String[] itemIDs = new String[pathSegmentsList.size()];
    for (int i = 0; i < itemIDs.length; i++) {
      itemIDs[i] = pathSegmentsList.get(i).getPath();
    }
    return Doubles.asList(getALSServingModel().dotProduct(userID, itemIDs));
  }
}
