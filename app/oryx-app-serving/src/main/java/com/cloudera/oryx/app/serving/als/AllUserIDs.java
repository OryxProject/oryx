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

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.Collection;

import com.cloudera.oryx.api.serving.OryxServingException;
import com.cloudera.oryx.app.serving.CSVMessageBodyWriter;

/**
 * <p>Responds to a GET request to {@code /user/allIDs}.</p>
 *
 * <p>CSV output consists of one ID per line. JSON output is an array of user IDs.</p>
 */
@Singleton
@Path("/user")
public final class AllUserIDs extends AbstractALSResource {

  @GET
  @Path("/allIDs")
  @Produces({MediaType.TEXT_PLAIN, CSVMessageBodyWriter.TEXT_CSV, MediaType.APPLICATION_JSON})
  public Collection<String> get() throws OryxServingException {
    return getALSServingModel().getAllUserIDs();
  }

}
