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

package com.cloudera.oryx.ml.serving;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;
import javax.servlet.ServletContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import com.cloudera.oryx.lambda.QueueProducer;
import com.cloudera.oryx.lambda.serving.ServingModelManager;

public abstract class AbstractOryxResource {

  public static final String MODEL_MANAGER_KEY =
      "com.cloudera.oryx.lambda.serving.ModelManagerListener.ModelManager";
  public static final String INPUT_PRODUCER_KEY =
      "com.cloudera.oryx.lambda.serving.ModelManagerListener.InputProducer";

  @Context
  private ServletContext servletContext;
  private QueueProducer<String,String> inputProducer;
  private ServingModelManager<?> servingModelManager;

  @SuppressWarnings("unchecked")
  protected void init() {
    servingModelManager = (ServingModelManager<?>)
        servletContext.getAttribute(MODEL_MANAGER_KEY);
    inputProducer = (QueueProducer<String,String>) servletContext.getAttribute(INPUT_PRODUCER_KEY);
  }

  protected final ServletContext getServletContext() {
    return servletContext;
  }

  protected ServingModelManager<?> getServingModelManager() {
    return servingModelManager;
  }

  protected final QueueProducer<?,String> getInputProducer() {
    return inputProducer;
  }

  protected static void check(boolean condition,
                              Response.Status status,
                              String errorMessage) throws OryxServingException {
    if (!condition) {
      throw new OryxServingException(status, errorMessage);
    }
  }

  protected static void check(boolean condition,
                              String errorMessage) throws OryxServingException {
    check(condition, Response.Status.BAD_REQUEST, errorMessage);
  }

  protected static void checkExists(boolean condition,
                                    String entity) throws OryxServingException {
    check(condition, Response.Status.NOT_FOUND, entity);
  }

  protected static InputStream maybeDecompress(String contentType,
                                               InputStream in) throws IOException {
    if (contentType != null) {
      switch (contentType) {
        case "application/zip":
          in = new ZipInputStream(in);
          break;
        case "application/gzip":
        case "application/x-gzip":
          in = new GZIPInputStream(in);
          break;
      }
    }
    return in;
  }


}
