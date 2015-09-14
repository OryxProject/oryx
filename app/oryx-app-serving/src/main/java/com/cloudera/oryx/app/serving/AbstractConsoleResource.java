/*
 * Copyright (c) 2015, Cloudera and Intel, Inc. All Rights Reserved.
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

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import com.google.common.io.Resources;

/**
 * Superclass of web resources that contain a small console for a Serving Layer application.
 * It applies a common header and footer around content from subclasses.
 */
public abstract class AbstractConsoleResource {

  private String html;

  @PostConstruct
  public final void loadHTML() throws IOException {
    StringBuilder htmlBuilder = new StringBuilder(10000);
    for (String resource : new String[] { "console-header.html.fragment",
                                          getConsoleResource(),
                                          "console-footer.html.fragment" }) {
      URL resourceURL = Resources.getResource(AbstractConsoleResource.class, resource);
      htmlBuilder.append(Resources.toString(resourceURL, StandardCharsets.UTF_8));
    }
    html = htmlBuilder.toString();
  }

  protected abstract String getConsoleResource();

  @GET
  @Produces(MediaType.TEXT_HTML)
  public final String getHTML(@Context HttpServletResponse response) {
    response.setHeader("X-Frame-Options", "SAMEORIGIN");
    response.setHeader("Cache-Control", "public");
    return html;
  }

}
