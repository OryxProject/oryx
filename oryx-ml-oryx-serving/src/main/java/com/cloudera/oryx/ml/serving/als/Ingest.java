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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.zip.GZIPInputStream;
import java.util.zip.InflaterInputStream;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.Part;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

import com.cloudera.oryx.lambda.QueueProducer;
import com.cloudera.oryx.ml.serving.OryxServingException;

/**
 * <p>Responds to a POST to {@code /ingest}. The content of the request are interpreted as
 * lines of text and are submitted to the input Kafka queue as-is.</p>
 *
 * <p>The body may be compressed with {@code gzip} or {@code deflate} {@code Content-Encoding}.
 * It may also by {@code multipart/form-data} encoded.</p>
 */
@Path("/ingest")
public final class Ingest extends AbstractALSResource {

  private static final Collection<String> INGEST_TYPES =
      Arrays.asList(MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON, TEXT_CSV);

  @POST
  @Consumes({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON, TEXT_CSV})
  public void post(InputStream in, @Context HttpHeaders requestHeaders) throws IOException {
    String contentEncoding =
        requestHeaders.getHeaderString(com.google.common.net.HttpHeaders.CONTENT_ENCODING);
    doPost(buildReader(contentEncoding, in));
  }

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
          String contentEncoding =
              part.getHeader(com.google.common.net.HttpHeaders.CONTENT_ENCODING);
          doPost(buildReader(contentEncoding, part.getInputStream()));
        }
      }
    }
    check(anyValidPart, "No Part with supported Content-Type");
  }

  private static BufferedReader buildReader(String contentEncoding,
                                            InputStream in) throws IOException {
    if (contentEncoding != null) {
      switch (contentEncoding) {
        case "deflate":
          in = new InflaterInputStream(in);
          break;
        case "gzip":
          in = new GZIPInputStream(in);
          break;
      }
    }
    return new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
  }

  private void doPost(BufferedReader buffered) throws IOException {
    QueueProducer<?,String> inputQueue = getInputProducer();
    String line;
    while ((line = buffered.readLine()) != null) {
      inputQueue.send(line);
    }
  }

}
