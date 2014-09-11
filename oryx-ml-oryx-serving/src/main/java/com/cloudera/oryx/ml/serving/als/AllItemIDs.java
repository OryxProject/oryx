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

import java.util.Collection;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.cloudera.oryx.ml.serving.CSVMessageBodyWriter;

/**
 * <p>Responds to a GET request to {@code /item/allIDs}.</p>
 *
 * <p>CSV output consists of one ID per line. JSON output is an array of item IDs.</p>
 */
@Path("/item")
public final class AllItemIDs extends AbstractALSResource {

  @GET
  @Path("/allIDs")
  @Produces({CSVMessageBodyWriter.TEXT_CSV, MediaType.APPLICATION_JSON})
  public Collection<String> get() {
    return getALSServingModel().getAllItemIDs();
  }
}
