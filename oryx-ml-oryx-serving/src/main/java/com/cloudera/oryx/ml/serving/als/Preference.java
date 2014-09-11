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

import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * <p>Responds to a POST request to {@code /pref/[userID]/[itemID]}. If the request body is empty,
 * the value is 1.0, otherwise the value in the request body's first line is used.</p>
 *
 * <p>Also responds to a DELETE request to the same path, with the same defaults.</p>
 */
@Path("/pref")
public final class Preference extends AbstractALSResource {

  @POST
  @Path("{userID}/{itemID}")
  public void post(@PathParam("userID") String userID,
                   @PathParam("itemID") String itemID) {

  }

  @DELETE
  @Path("{userID}/{itemID}")
  public void delete(@PathParam("userID") String userID,
                     @PathParam("itemID") String itemID) {

  }

}
