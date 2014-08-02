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

package com.cloudera.oryx.lambda.serving.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class OryxExceptionMapper implements ExceptionMapper<OryxException> {

  private static final Logger LOG = LoggerFactory.getLogger(OryxExceptionMapper.class);

  @Override
  public Response toResponse(OryxException ex) {
    LOG.error("+ Exception in Serving Layer Operation, Cause: <{}>, Message: <{}>", ex.getCause(), ex.getMessage());
    return Response.status(500).entity(ex.getMessage()).type(MediaType.TEXT_PLAIN).build();
  }
}
