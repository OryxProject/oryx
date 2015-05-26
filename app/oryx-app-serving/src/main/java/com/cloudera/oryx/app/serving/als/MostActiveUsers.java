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
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.List;

import com.cloudera.oryx.app.als.Rescorer;
import com.cloudera.oryx.app.als.RescorerProvider;
import com.cloudera.oryx.app.serving.CSVMessageBodyWriter;
import com.cloudera.oryx.app.serving.IDCount;
import com.cloudera.oryx.app.serving.OryxServingException;
import com.cloudera.oryx.app.serving.als.model.ALSServingModel;

/**
 * <p>Responds to a GET request to
 * {@code /mostActiveUsers(?howMany=n)(&offset=o)(&rescorerParams=...)}</p>
 *
 * <p>Results are users that have interacted with the most items, as user and count pairs.</p>
 *
 * <p>{@code howMany} and {@code offset} behavior are as in {@link Recommend}. Output
 * is also the same, except that user IDs are returned with integer counts rather than
 * scores.</p>
 *
 * @see MostPopularItems
 */
@Singleton
@Path("/mostActiveUsers")
public final class MostActiveUsers extends AbstractALSResource {

  @GET
  @Produces({MediaType.TEXT_PLAIN, CSVMessageBodyWriter.TEXT_CSV, MediaType.APPLICATION_JSON})
  public List<IDCount> get(@DefaultValue("10") @QueryParam("howMany") int howMany,
                           @DefaultValue("0") @QueryParam("offset") int offset,
                           @QueryParam("rescorerParams") List<String> rescorerParams)
      throws OryxServingException {
    ALSServingModel model = getALSServingModel();
    RescorerProvider rescorerProvider = model.getRescorerProvider();
    Rescorer rescorer = null;
    if (rescorerProvider != null) {
      rescorer = rescorerProvider.getMostActiveUsersRescorer(rescorerParams);
    }
    return MostPopularItems.mapTopCountsToIDCounts(
        model.getUserCounts(), howMany, offset, rescorer);
  }

}
