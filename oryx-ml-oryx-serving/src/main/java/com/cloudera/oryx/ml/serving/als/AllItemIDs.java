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
import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import com.cloudera.oryx.ml.serving.als.model.ALSServingModel;
import com.cloudera.oryx.ml.serving.als.model.ALSServingModelManager;

/**
 * <p>Responds to a GET request to {@code /item/allIDs}
 * and in turn calls {link com.cloudera.oryx.ml.serving.als.model.ALSServingModel#getAllItemIDs()}.</p>
 * <p/>
 * <p>JSON output is an array of item IDs.</p>
 */
@Path("/item")
public final class AllItemIDs {

  @Context
  private ServletContext servletContext;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/allIDs")
  public List<Integer> getAllItemIDs() {
    ALSServingModelManager alsServingModelManager =
        (ALSServingModelManager) servletContext.getAttribute("ModelManager");
    ALSServingModel alsServingModel = alsServingModelManager.getModel();
    return alsServingModel.getAllItemIDs();
  }
}
