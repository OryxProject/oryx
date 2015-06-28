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

import java.util.ArrayList;
import java.util.List;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.cloudera.oryx.app.serving.OryxServingException;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.app.serving.CSVMessageBodyWriter;
import com.cloudera.oryx.app.serving.als.model.ALSServingModel;

/**
 * <p>Responds to a GET request to {@code /popularRepresentativeItems}.</p>
 *
 * <p>The result is a list of items that is in some way representative of the range of
 * items in the model. That is, the items will tend to be different from each other,
 * and popular. Specifically, it is "recommending" one item to each of the latent
 * features in the model.</p>
 *
 * <p>Output is one item ID per line, or in the case of JSON output, an array of IDs.</p>
 */
@Singleton
@Path("/popularRepresentativeItems")
public final class PopularRepresentativeItems extends AbstractALSResource {

  @GET
  @Produces({MediaType.TEXT_PLAIN, CSVMessageBodyWriter.TEXT_CSV, MediaType.APPLICATION_JSON})
  public List<String> get() throws OryxServingException {
    ALSServingModel model = getALSServingModel();
    int features = model.getFeatures();
    List<String> items = new ArrayList<>(features);
    float[] unitVector = new float[features];
    for (int i = 0; i < features; i++) {
      unitVector[i] = 1.0f;
      List<Pair<String,Double>> topIDDot = model.topN(new DotsFunction(unitVector), null, 1, null);
      items.add(topIDDot.isEmpty() ? null : topIDDot.get(0).getFirst());
      unitVector[i] = 0.0f; // reset
    }
    return items;
  }

}
