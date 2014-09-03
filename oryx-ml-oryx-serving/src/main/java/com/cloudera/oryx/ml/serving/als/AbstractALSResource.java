/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
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

import javax.annotation.PostConstruct;
import javax.servlet.ServletContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import com.cloudera.oryx.lambda.QueueProducer;
import com.cloudera.oryx.lambda.serving.ServingModelManager;
import com.cloudera.oryx.ml.serving.OryxServingException;
import com.cloudera.oryx.ml.serving.als.model.ALSServingModel;

public abstract class AbstractALSResource {

  public static final String MODEL_MANAGER_KEY =
      "com.cloudera.oryx.lambda.serving.ModelManagerListener.ModelManager";
  public static final String INPUT_PRODUCER_KEY =
      "com.cloudera.oryx.lambda.serving.ModelManagerListener.InputProducer";

  @Context
  private ServletContext servletContext;
  private ALSServingModel alsServingModel;
  private QueueProducer<String,String> inputProducer;

  @SuppressWarnings("unchecked")
  @PostConstruct
  public final void init() {
    ServingModelManager<?> servingModelManager = (ServingModelManager<?>)
        servletContext.getAttribute(MODEL_MANAGER_KEY);
    alsServingModel = (ALSServingModel) servingModelManager.getModel();
    inputProducer = (QueueProducer<String,String>) servletContext.getAttribute(INPUT_PRODUCER_KEY);
  }

  protected final ALSServingModel getALSServingModel() {
    return alsServingModel;
  }

  protected final QueueProducer<String,String> getInputProducer() {
    return inputProducer;
  }

  protected final void check(boolean condition,
                             Response.Status status,
                             String errorMessage) throws OryxServingException {
    if (!condition) {
      throw new OryxServingException(status, errorMessage);
    }
  }

  protected final void check(boolean condition,
                             String errorMessage) throws OryxServingException {
    check(condition, Response.Status.BAD_REQUEST, errorMessage);
  }

}
