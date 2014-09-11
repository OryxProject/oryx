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

import java.util.Arrays;
import java.util.List;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.PathSegment;

import com.cloudera.oryx.ml.serving.IDValue;

@Path("/similarity")
public final class Similarity extends AbstractALSResource {

  @GET
  @Path("{itemID : .+}")
  @Produces(MediaType.APPLICATION_JSON)
  public List<IDValue> get(@PathParam("itemID") List<PathSegment> pathSegmentsList,
                           @DefaultValue("10") @QueryParam("howMany") int howMany,
                           @DefaultValue("0") @QueryParam("offset") int offset,
                           @QueryParam("rescorerParams") List<String> rescorerParams) {
    return Arrays.asList(new IDValue("1", 5));

  }

}
