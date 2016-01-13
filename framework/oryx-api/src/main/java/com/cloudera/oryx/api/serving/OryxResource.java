/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
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

package com.cloudera.oryx.api.serving;

import javax.annotation.PostConstruct;
import javax.servlet.ServletContext;
import javax.ws.rs.core.Context;

import java.util.Objects;

import com.cloudera.oryx.api.TopicProducer;

/**
 * A utility class that can serve as a superclass of Serving Layer application endpoints.
 * It handles loading provided objects like a {@link ServingModelManager}.
 *
 * @since 2.0.0
 */
public abstract class OryxResource {

  public static final String MODEL_MANAGER_KEY =
      "com.cloudera.oryx.lambda.serving.ModelManagerListener.ModelManager";
  public static final String INPUT_PRODUCER_KEY =
      "com.cloudera.oryx.lambda.serving.ModelManagerListener.InputProducer";

  @Context
  private ServletContext servletContext;
  private ServingModelManager<?> servingModelManager;
  private TopicProducer<?,?> inputProducer;

  @SuppressWarnings("unchecked")
  @PostConstruct
  protected void init() {
    servingModelManager = Objects.requireNonNull(
        (ServingModelManager<?>) servletContext.getAttribute(MODEL_MANAGER_KEY),
        "No ServingModelManager");
    inputProducer = Objects.requireNonNull(
        (TopicProducer<?,?>) servletContext.getAttribute(INPUT_PRODUCER_KEY),
        "No input producer available; read-only mode?");
  }

  /**
   * @return a reference to the {@link ServingModelManager} for the app, configured in the
   *  {@link ServletContext} under key {@link #MODEL_MANAGER_KEY}
   * @since 2.0.0
   */
  protected final ServingModelManager<?> getServingModelManager() {
    return servingModelManager;
  }

  /**
   * @return a reference to the {@link TopicProducer} for the app, configured in the
   *  {@link ServletContext} under key {@link #INPUT_PRODUCER_KEY}
   * @since 2.0.0
   */
  protected final TopicProducer<?,?> getInputProducer() {
    return inputProducer;
  }

}
