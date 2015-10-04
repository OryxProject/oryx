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

package com.cloudera.oryx.app.serving;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;
import javax.annotation.PostConstruct;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import com.google.common.base.Preconditions;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.FileCleanerCleanup;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.api.TopicProducer;
import com.cloudera.oryx.api.serving.ServingModel;
import com.cloudera.oryx.api.serving.ServingModelManager;

/**
 * Superclass of all Serving Layer application endpoints.
 */
public abstract class AbstractOryxResource {

  private static final Logger log = LoggerFactory.getLogger(AbstractOryxResource.class);

  public static final String MODEL_MANAGER_KEY =
      "com.cloudera.oryx.lambda.serving.ModelManagerListener.ModelManager";
  public static final String INPUT_PRODUCER_KEY =
      "com.cloudera.oryx.lambda.serving.ModelManagerListener.InputProducer";

  private static final AtomicReference<DiskFileItemFactory> sharedFileItemFactory =
      new AtomicReference<>();

  @Context
  private ServletContext servletContext;
  private TopicProducer<String,String> inputProducer;
  private ServingModelManager<?> servingModelManager;
  private boolean hasLoadedEnough;

  @SuppressWarnings("unchecked")
  @PostConstruct
  protected void init() {
    servingModelManager = (ServingModelManager<?>) servletContext.getAttribute(MODEL_MANAGER_KEY);
    inputProducer = (TopicProducer<String,String>) servletContext.getAttribute(INPUT_PRODUCER_KEY);
  }

  protected final void sendInput(String message) {
    inputProducer.send(Integer.toHexString(message.hashCode()), message);
  }

  protected final boolean isReadOnly() {
    return servingModelManager.isReadOnly();
  }

  protected final ServingModel getServingModel() throws OryxServingException {
    ServingModel servingModel = servingModelManager.getModel();
    if (hasLoadedEnough) {
      Objects.requireNonNull(servingModel);
      return servingModel;
    }
    if (servingModel != null) {
      double minModelLoadFraction =
          servingModelManager.getConfig().getDouble("oryx.serving.min-model-load-fraction");
      Preconditions.checkArgument(minModelLoadFraction >= 0.0 && minModelLoadFraction <= 1.0);
      float fractionLoaded = servingModel.getFractionLoaded();
      log.info("Model loaded fraction: {}", fractionLoaded);
      if (fractionLoaded >= minModelLoadFraction) {
        hasLoadedEnough = true;
      }
    }
    if (hasLoadedEnough) {
      Objects.requireNonNull(servingModel);
      return servingModel;
    } else {
      throw new OryxServingException(Response.Status.SERVICE_UNAVAILABLE);
    }
  }

  protected final List<FileItem> parseMultipart(HttpServletRequest request)
      throws OryxServingException {

    // JAX-RS does not by itself support multipart form data yet, so doing it manually.
    // We'd use Servlet 3.0 but the Grizzly test harness doesn't let us test it :(
    // Good old Commons FileUpload it is:

    if (sharedFileItemFactory.get() == null) {
      // Not a big deal if two threads actually set this up
      DiskFileItemFactory fileItemFactory = new DiskFileItemFactory(
          1 << 16, (File) servletContext.getAttribute("javax.servlet.context.tempdir"));
      fileItemFactory.setFileCleaningTracker(
          FileCleanerCleanup.getFileCleaningTracker(servletContext));
      sharedFileItemFactory.compareAndSet(null, fileItemFactory);
    }

    List<FileItem> fileItems;
    try {
      fileItems = new ServletFileUpload(sharedFileItemFactory.get()).parseRequest(request);
    } catch (FileUploadException e) {
      throw new OryxServingException(Response.Status.BAD_REQUEST, e.getMessage());
    }
    check(!fileItems.isEmpty(), "No parts");
    return fileItems;
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

  protected void checkNotReadOnly() throws OryxServingException {
    check(!isReadOnly(), Response.Status.FORBIDDEN, "Serving Layer is read-only");
  }

  protected static BufferedReader maybeBuffer(InputStream in) {
    return maybeBuffer(new InputStreamReader(in, StandardCharsets.UTF_8));
  }

  protected static BufferedReader maybeBuffer(Reader reader) {
    return reader instanceof BufferedReader ? (BufferedReader) reader : new BufferedReader(reader);
  }

  protected static InputStream maybeDecompress(FileItem item) throws IOException {
    InputStream in = item.getInputStream();
    String contentType = item.getContentType();
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
