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

package com.cloudera.oryx.app.serving.kmeans;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.junit.Assert;
import org.junit.Test;

public final class ConsoleTest extends AbstractKMeansServingTest {

  @Test
  public void testConsole() {
    Response response = target("/index.html").request().accept(MediaType.TEXT_HTML).get();
    Assert.assertEquals("public", response.getHeaderString("Cache-Control"));
    Assert.assertEquals("SAMEORIGIN", response.getHeaderString("X-Frame-Options"));
    String html = response.readEntity(String.class);
    String[] substrings = {
        "DOCTYPE",
        "Oryx Serving Layer",
        "K-means",
        "/assign",
        "<form"
    };
    for (String substring : substrings) {
      Assert.assertTrue("Doesn't contain " + substring, html.contains(substring));
    }
  }

}
