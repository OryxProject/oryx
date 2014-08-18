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

import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.cloudera.oryx.lambda.serving.ModelManagerListener;
import com.cloudera.oryx.ml.serving.als.model.ALSServingModel;
import com.cloudera.oryx.ml.serving.als.model.ALSServingModelManager;

/**
 * <p>Responds to a HEAD or GET request to {@code /ready}
 * Returns "OK" or "Unavailable" status depending on
 * whether the ALSServingModel is available or not.</p>
 */
@Path("/ready")
public final class Ready {

  @Context
  private ServletContext servletContext;

  @GET
  @HEAD
  @Produces(MediaType.APPLICATION_JSON)
  public Response get() {

    ALSServingModelManager alsServingModelManager =
        (ALSServingModelManager) servletContext.getAttribute(ModelManagerListener.MANAGER_KEY);
    ALSServingModel alsServingModel = alsServingModelManager.getModel();

    if (alsServingModel != null) {
      return Response.ok().build();
    } else {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
    }
  }
}
