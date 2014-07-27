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

package com.cloudera.oryx.serving.web;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * <p>Responds to a GET request to {@code /popularRepresentativeItems}
 * and in turn calls {link OryxRecommender#popularRepresentativeItems()}.</p>
 * <p/>
 * <p>Output is one item ID per line, or in the case of JSON output, an array of IDs.</p>
 */
@Path("/popularRepresentativeItems")
public class PopularRepresentativeItems {

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public List<String> get() {
/*
    OryxRecommender recommender = getRecommender();
    try {
      output(request, response, recommender.popularRepresentativeItems());
    } catch (NotReadyException nre) {
      response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, nre.toString());
    } catch (IllegalArgumentException iae) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, iae.toString());
    }
  */
    return new ArrayList<>(Arrays.asList("1", "2"));
  }

}
