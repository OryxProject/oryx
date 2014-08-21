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

import com.cloudera.oryx.lambda.serving.ModelManagerListener;
import com.cloudera.oryx.lambda.serving.ServingModelManager;
import com.cloudera.oryx.ml.serving.als.model.ALSServingModel;

public abstract class AbstractALSResource {

  @Context
  private ServletContext servletContext;

  private ALSServingModel alsServingModel;

  @PostConstruct
  public void init() {
    ServingModelManager<?> servingModelManager =
        (ServingModelManager<?>) servletContext.getAttribute(ModelManagerListener.MANAGER_KEY);
    alsServingModel = (ALSServingModel) servingModelManager.getModel();
  }

  protected ALSServingModel getALSServingModel() {
    return alsServingModel;
  }
}
