/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
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

package com.cloudera.oryx.example.serving;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;

import com.cloudera.oryx.api.TopicProducer;
import com.cloudera.oryx.api.serving.OryxResource;

/**
 * Responds to a POST request to {@code /add/[line]}. Adds a new line of input for processing.
 * Also responds to a POST to {@code /add} which contains lines in the request body.
 */
@Path("/add")
public final class Add extends OryxResource {

  @POST
  @Path("{line}")
  public void add(@PathParam("line") String line) {
    getProducer().send(null, line);
  }

  @POST
  @Consumes({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
  public void post(Reader reader) throws IOException {
    TopicProducer<?,String> inputProducer = getProducer();
    BufferedReader buffered = new BufferedReader(reader);
    for (String line; (line = buffered.readLine()) != null;) {
      inputProducer.send(null, line);
    }
  }

  private TopicProducer<?,String> getProducer() {
    @SuppressWarnings("unchecked")
    TopicProducer<?,String> inputProducer = (TopicProducer<?,String>) getInputProducer();
    return inputProducer;
  }

}
