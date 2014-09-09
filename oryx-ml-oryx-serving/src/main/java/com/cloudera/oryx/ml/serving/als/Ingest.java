/*
 * Copyright (c) 2014, Cloudera, Inc. and Intel Corp. All Rights Reserved.
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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;
import javax.annotation.PostConstruct;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.FileCleanerCleanup;
import org.apache.commons.fileupload.servlet.ServletFileUpload;

import com.cloudera.oryx.lambda.QueueProducer;
import com.cloudera.oryx.ml.serving.CSVMessageBodyWriter;
import com.cloudera.oryx.ml.serving.OryxServingException;

/**
 * <p>Responds to a POST to {@code /ingest}. The content of the request are interpreted as
 * lines of text and are submitted to the input Kafka queue as-is.</p>
 *
 * <p>The body may be compressed with {@code gzip} or {@code deflate} {@code Content-Encoding}.
 * It may also by {@code multipart/form-data} encoded, and each part may be compressed with
 * {@code Content-Type} {@code application/zip}, {@code application/gzip}, or
 * {@code application/x-gzip}.</p>
 *
 * <p>In all events the uncompressed data should be text, encoding with UTF-8, and with
 * {@code \n} line separators.</p>
 */
@Path("/ingest")
public final class Ingest extends AbstractALSResource {

  private DiskFileItemFactory fileItemFactory;

  @Override
  @PostConstruct
  public void init() {
    super.init();
    ServletContext context = getServletContext();
    fileItemFactory = new DiskFileItemFactory(
        1 << 16, (File) context.getAttribute("javax.servlet.context.tempdir"));
    fileItemFactory.setFileCleaningTracker(FileCleanerCleanup.getFileCleaningTracker(context));
  }

  @POST
  @Consumes({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON, CSVMessageBodyWriter.TEXT_CSV})
  public void post(Reader reader) throws IOException {
    doPost(reader instanceof BufferedReader ? (BufferedReader) reader : new BufferedReader(reader));
  }

  @POST
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  public void post(@Context HttpServletRequest request)
      throws IOException, FileUploadException, OryxServingException {
    // JAX-RS does not by itself support multipart form data yet, so doing it manually.
    // We'd use Servlet 3.0 but the Grizzly test harness doesn't let us test it :(
    // Good old Commons FileUpload it is:
    List<FileItem> fileItems = new ServletFileUpload(fileItemFactory).parseRequest(request);
    check(!fileItems.isEmpty(), "No parts");
    for (FileItem item : fileItems) {
      InputStream in = maybeDecompress(item.getContentType(), item.getInputStream());
      doPost(new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8)));
    }
  }

  private void doPost(BufferedReader buffered) throws IOException {
    QueueProducer<?,String> inputQueue = getInputProducer();
    String line;
    while ((line = buffered.readLine()) != null) {
      inputQueue.send(line);
    }
  }

  private static InputStream maybeDecompress(String contentType,
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

  /*
  @POST
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  public void post(@Context HttpServletRequest request)
      throws IOException, ServletException, OryxServingException {
    // JAX-RS does not by itself support multipart form data yet, so doing it manually:
    Collection<Part> parts = request.getParts();
    boolean anyValidPart = false;
    if (parts != null) {
      for (Part part : parts) {
        String partContentType = part.getContentType();
        if (INGEST_TYPES.contains(partContentType)) {
          anyValidPart = true;
          doPost(new BufferedReader(new InputStreamReader(part.getInputStream(), StandardCharsets.UTF_8)));
        }
      }
    }
    check(anyValidPart, "No Part with supported Content-Type");
  }
   */

}
