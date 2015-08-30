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

package com.cloudera.oryx.lambda.serving;

import java.io.PrintWriter;
import java.io.StringWriter;
import javax.servlet.RequestDispatcher;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.catalina.manager.JspHelper;

/**
 * Constructs a simple HTML error page from error info in the request.
 * This is intended to be where the container forwards errored requests to.
 */
@Path("/error")
public final class ErrorResource {

  @GET
  @Produces(MediaType.TEXT_HTML)
  public Response errorHTML(@Context HttpServletRequest request) {
    StringBuilder html = new StringBuilder(1000);

    html.append("<!DOCTYPE html>" +
        "<html><head><title>Error</title>" +
        "<style type=\"text/css\">" +
        "body{background-color:#01596e} body,p{font-family:monospace;color:white}" +
        "</style></head>" +
        "<body>");

    html.append("<p><strong>Error");
    Number statusCode = (Number) request.getAttribute(RequestDispatcher.ERROR_STATUS_CODE);
    if (statusCode != null) {
      html.append(' ').append(statusCode);
    }
    html.append("</strong>");
    Object requestURI = request.getAttribute(RequestDispatcher.ERROR_REQUEST_URI);
    if (requestURI != null) {
      html.append(" : ");
      html.append(JspHelper.escapeXml(requestURI));
    }
    html.append("</p>");

    Object message = request.getAttribute(RequestDispatcher.ERROR_MESSAGE);
    if (message != null) {
      html.append("<p><strong>");
      html.append(JspHelper.escapeXml(message));
      html.append("</strong></p>");
    }

    Throwable throwable = (Throwable) request.getAttribute(RequestDispatcher.ERROR_EXCEPTION);
    if (throwable != null) {
      StringWriter sw = new StringWriter();
      throwable.printStackTrace(new PrintWriter(sw));
      html.append("<p><pre>");
      html.append(JspHelper.escapeXml(sw.toString()));
      html.append("</pre></p>");
    }

    html.append("</body></html>");

    Response.Status finalStatus = statusCode == null ?
        Response.Status.OK : Response.Status.fromStatusCode(statusCode.intValue());
    return Response.status(finalStatus).entity(html.toString()).build();
  }

  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public Response errorText(@Context HttpServletRequest request) {
    StringBuilder text = new StringBuilder(1000);

    text.append("Error");
    Number statusCode = (Number) request.getAttribute(RequestDispatcher.ERROR_STATUS_CODE);
    if (statusCode != null) {
      text.append(' ').append(statusCode);
    }
    Object requestURI = request.getAttribute(RequestDispatcher.ERROR_REQUEST_URI);
    if (requestURI != null) {
      text.append(" : ");
      text.append(requestURI);
    }
    text.append('\n');

    Object message = request.getAttribute(RequestDispatcher.ERROR_MESSAGE);
    if (message != null) {
      text.append(message).append('\n');
    }

    Throwable throwable = (Throwable) request.getAttribute(RequestDispatcher.ERROR_EXCEPTION);
    if (throwable != null) {
      StringWriter sw = new StringWriter();
      throwable.printStackTrace(new PrintWriter(sw));
      text.append(sw);
    }

    Response.Status finalStatus = statusCode == null ?
        Response.Status.OK : Response.Status.fromStatusCode(statusCode.intValue());
    return Response.status(finalStatus).entity(text.toString()).build();
  }

  @GET
  @Produces(MediaType.WILDCARD)
  public Response errorEmpty(@Context HttpServletRequest request) {
    Number statusCode = (Number) request.getAttribute(RequestDispatcher.ERROR_STATUS_CODE);
    Response.Status finalStatus = statusCode == null ?
        Response.Status.OK : Response.Status.fromStatusCode(statusCode.intValue());
    return Response.status(finalStatus).build();
  }

}