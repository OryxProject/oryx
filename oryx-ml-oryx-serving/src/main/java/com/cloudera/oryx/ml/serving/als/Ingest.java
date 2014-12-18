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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import javax.annotation.PostConstruct;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.base.Splitter;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.FileCleanerCleanup;
import org.apache.commons.fileupload.servlet.ServletFileUpload;

import com.cloudera.oryx.lambda.TopicProducer;
import com.cloudera.oryx.ml.serving.CSVMessageBodyWriter;
import com.cloudera.oryx.ml.serving.OryxServingException;

/**
 * <p>Responds to a POST to {@code /ingest}. For each line in the request body, a line of CSV text
 * is written to the input Kafka topic, in the form {@code userID,itemID,strength,timestamp}.
 * Strength must be a number and defaults to 1; an empty strength value signifies a delete.
 * Timestamp must be a number of milliseconds since Jany 1, 1970.
 * These values are parsed directly from the request body line, which may be in one of
 * several forms:</p>

 * <ul>
 *   <li>{@code userID,itemID}: strength defaults to 1 and timestamp is current system time</li>
 *   <li>{@code userID,itemID,}: interpreted as a "delete" for the user-item association.
 *     timestamp is current system time</li>
 *   <li>{@code userID,itemID,strength}: timestamp is current system time</li>
 *   <li>{@code userID,itemID,strength,timestamp}: all given values are parsed and used,
 *     including timestamp.</li>
 * </ul>
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

  // Use Splitter to handle blank trailing field, unlike Pattern
  private static final Splitter COMMA = Splitter.on(',');

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
  @Consumes({MediaType.TEXT_PLAIN, CSVMessageBodyWriter.TEXT_CSV, MediaType.APPLICATION_JSON})
  public void post(Reader reader) throws IOException, OryxServingException {
    doPost(maybeBuffer(reader));
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

  private void doPost(BufferedReader buffered) throws IOException, OryxServingException {
    TopicProducer<?,String> inputTopic = getInputProducer();
    String line;
    while ((line = buffered.readLine()) != null) {
      Iterator<String> tokens = COMMA.split(line).iterator();
      check(tokens.hasNext(), line);
      String userID = tokens.next();
      check(tokens.hasNext(), line);
      String itemID = tokens.next();
      String strength;
      long timestamp;
      // Has a strength?
      if (tokens.hasNext()) {
        String rawStrength = tokens.next();
        // Special case deletes:
        if (rawStrength.isEmpty()) {
          strength = "";
        } else {
          strength = Preference.validateAndStandardizeStrength(rawStrength);
        }
        // Has a timestamp?
        if (tokens.hasNext()) {
          try {
            timestamp = Long.parseLong(tokens.next());
          } catch (NumberFormatException nfe) {
            throw new OryxServingException(Response.Status.BAD_REQUEST, nfe.getMessage());
          }
          check(timestamp > 0, line);
        } else {
          timestamp = System.currentTimeMillis();
        }
      } else {
        strength = "1";
        timestamp = System.currentTimeMillis();
      }
      inputTopic.send(userID + "," + itemID + "," + strength + "," + timestamp);
    }
  }
}
