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

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

/**
 * <p>Responds to a POST request to {@code /ingest} and in turn calls
 * {link OryxRecommender#ingest(Reader)}}. The content of the request body is
 * fed to this method. Note that the content may be gzipped; if so, header "Content-Encoding"
 * must have value "gzip".</p>
 * <p/>
 * <p>Alternatively, CSV data may be POSTed here as if part of a web browser file upload. In this case
 * the "Content-Type" should be "multipart/form-data", and the payload encoded accordingly. The uploaded
 * file may be gzipped or zipped.</p>
 */
@Path("/ingest")
public final class Ingest {

  @POST
  @Path("{userId}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response post(@PathParam("userId") String userId,
                       @QueryParam("howMany") int howMany,
                       @QueryParam("offset") int offset,
                       @QueryParam("considerKnownItems") boolean considerKnownItems,
                       @QueryParam("rescorerParams") List<String> rescorerParams) {
/*
    OryxRecommender recommender = getRecommender();

    String contentType = request.getContentType();
    boolean fromBrowserUpload = contentType != null && contentType.startsWith("multipart/form-data");

    Reader reader;
    if (fromBrowserUpload) {

      Collection<Part> parts = request.getParts();
      if (parts == null || parts.isEmpty()) {
        response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No form data");
        return;
      }
      Part part = parts.iterator().next();
      String partContentType = part.getContentType();
      InputStream in = part.getInputStream();
      if ("application/zip".equals(partContentType)) {
        in = new ZipInputStream(in);
      } else if ("application/gzip".equals(partContentType)) {
        in = new GZIPInputStream(in);
      } else if ("application/x-gzip".equals(partContentType)) {
        in = new GZIPInputStream(in);
      }
      reader = new InputStreamReader(in, Charsets.UTF_8);

    } else {

      String charEncodingName = request.getCharacterEncoding();
      Charset charEncoding = charEncodingName == null ? Charsets.UTF_8 : Charset.forName(charEncodingName);
      String contentEncoding = request.getHeader(HttpHeaders.CONTENT_ENCODING);
      if (contentEncoding == null) {
        reader = request.getReader();
      } else if ("gzip".equals(contentEncoding)) {
        reader = new InputStreamReader(new GZIPInputStream(request.getInputStream()), charEncoding);
      } else if ("zip".equals(contentEncoding)) {
        reader = new InputStreamReader(new ZipInputStream(request.getInputStream()), charEncoding);
      } else {
        response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Unsupported Content-Encoding");
        return;
      }
    }

    try {
      recommender.ingest(reader);
    } catch (IllegalArgumentException iae) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, iae.toString());
      return;
    } catch (NoSuchElementException nsee) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, nsee.toString());
      return;
    }

    String referer = request.getHeader(HttpHeaders.REFERER);
    if (fromBrowserUpload && referer != null) {
      // Parsing avoids response splitting
      response.sendRedirect(new URL(referer).toString());
    }
  */
    return Response.status(200).entity("").build();
  }

}
