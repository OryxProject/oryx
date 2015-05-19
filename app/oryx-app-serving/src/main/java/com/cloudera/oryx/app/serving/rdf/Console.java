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

package com.cloudera.oryx.app.serving.rdf;

import javax.ws.rs.Path;

import com.cloudera.oryx.app.serving.AbstractConsoleResource;

/**
 * Random decision forest app Serving Layer console.
 */
@Path("/{file:(index\\.html)?}")
public final class Console extends AbstractConsoleResource {
  @Override
  protected String getConsoleResource() {
    return "rdf/rdf.html.fragment";
  }
}
