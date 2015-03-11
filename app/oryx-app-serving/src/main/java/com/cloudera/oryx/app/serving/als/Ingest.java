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

package com.cloudera.oryx.app.serving.als;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.fileupload.FileItem;

import com.cloudera.oryx.api.TopicProducer;
import com.cloudera.oryx.app.serving.CSVMessageBodyWriter;
import com.cloudera.oryx.app.serving.OryxServingException;
import com.cloudera.oryx.common.text.TextUtils;

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

  @POST
  @Consumes({MediaType.TEXT_PLAIN, CSVMessageBodyWriter.TEXT_CSV, MediaType.APPLICATION_JSON})
  public void post(Reader reader) throws IOException, OryxServingException {
    doPost(maybeBuffer(reader));
  }

  @POST
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  public void post(@Context HttpServletRequest request) throws IOException, OryxServingException {
    for (FileItem item : parseMultipart(request)) {
      InputStream in = maybeDecompress(item.getContentType(), item.getInputStream());
      try (BufferedReader reader = maybeBuffer(new InputStreamReader(in, StandardCharsets.UTF_8))) {
        doPost(reader);
      }
    }
  }

  private void doPost(BufferedReader buffered) throws IOException, OryxServingException {
    TopicProducer<?,String> inputTopic = getInputProducer();
    String line;
    while ((line = buffered.readLine()) != null) {
      String[] tokens = TextUtils.parseDelimited(line, ',');
      check(tokens.length >= 2, line);
      String userID = tokens[0];
      String itemID = tokens[1];
      String strength;
      long timestamp;
      // Has a strength?
      if (tokens.length >= 3) {
        String rawStrength = tokens[2];
        // Special case deletes:
        if (rawStrength.isEmpty()) {
          strength = "";
        } else {
          strength = Preference.validateAndStandardizeStrength(rawStrength);
        }
        // Has a timestamp?
        if (tokens.length >= 4) {
          try {
            timestamp = Long.parseLong(tokens[3]);
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
