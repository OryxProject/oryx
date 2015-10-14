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

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import com.cloudera.oryx.api.TopicProducer;
import com.cloudera.oryx.api.serving.OryxResource;

/**
 * Responds to a POST request to {@code /add/[word]}. Increments the count for the word.
 */
@Path("/add")
public final class Add extends OryxResource {

  @POST
  @Path("{word}")
  public void add(@PathParam("word") String word) {
    @SuppressWarnings("unchecked")
    TopicProducer<?,String> inputProducer = (TopicProducer<?,String>) getInputProducer();
    inputProducer.send(null, word);
  }

}
