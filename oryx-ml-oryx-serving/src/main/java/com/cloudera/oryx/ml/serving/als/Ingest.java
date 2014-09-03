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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.Part;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.ml.serving.als.model.ALSServingModel;

/**
 * <p>Responds to a POST request to {@code /ingest} and in turn calls
 * {link ALSServingModel#ingest(Reader)}}. The content of the request body is
 * fed to this method. Note that the content may be gzipped; if so, header "Content-Encoding"
 * must have value "gzip" or "x-gzip".</p>
 *
 * <p>Alternatively, CSV data may be POSTed here as if part of a web browser file upload. In this case
 * the "Content-Type" should be "multipart/form-data", and the payload encoded accordingly. The uploaded
 * file may be gzipped or zipped.</p>
 */
@Path("/ingest")
public final class Ingest extends AbstractALSResource {

  private static final Logger log = LoggerFactory.getLogger(Ingest.class);

  @Context
  private HttpServletRequest httpServletRequest;

  @Context
  private HttpHeaders httpHeaders;

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes({MediaType.MULTIPART_FORM_DATA, MediaType.TEXT_PLAIN})
  public Response post() {
    ALSServingModel alsServingModel = getALSServingModel();
    String contentType = httpServletRequest.getContentType();
    boolean fromBrowserUpload = contentType != null && contentType.startsWith(MediaType.MULTIPART_FORM_DATA);

    Reader reader;
    try {
      if (fromBrowserUpload) {
        Collection<Part> parts = httpServletRequest.getParts();
        if (parts == null || parts.isEmpty()) {
          return Response.status(Response.Status.BAD_REQUEST).entity("No Form Data").build();
        }

        Part part = parts.iterator().next();
        InputStream in = part.getInputStream();
        reader = new InputStreamReader(in, Charsets.UTF_8);
      } else {
        String contentEncoding = httpServletRequest.getHeader(HttpHeaders.CONTENT_ENCODING);
        if (contentEncoding == null) {
          reader = httpServletRequest.getReader();
        }
      }
    } catch (ServletException | IOException ex) {
      return Response.status(Response.Status.BAD_REQUEST).entity(ex.getMessage()).build();
    }

//    try {
//      alsServingModel.ingest(reader);
//    } catch (IllegalArgumentException | NoSuchElementException ex) {
//      return Response.status(Response.Status.BAD_REQUEST).entity(ex.toString()).build();
//    }

    String referrer = httpHeaders.getRequestHeader("referer").get(0);
    if (fromBrowserUpload && referrer != null) {
      // Parsing avoids response splitting
      try {
        return Response.created(new URI(referrer)).build();
      } catch (URISyntaxException e) {
        log.error("Unable to read referring URI: {}", referrer);
      }
    }
    return Response.ok().entity("").build();
  }
}
