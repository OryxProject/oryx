/*
 * Copyright (c) 2016, Cloudera, Inc. All Rights Reserved.
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

import javax.ws.rs.core.Response;

import org.junit.Test;

import com.cloudera.oryx.api.serving.OryxServingException;
import com.cloudera.oryx.common.OryxTest;

public final class OryxExceptionMapperTest extends OryxTest {

  @Test
  public void testMap() {
    OryxExceptionMapper mapper = new OryxExceptionMapper();
    OryxServingException ex = new OryxServingException(Response.Status.BAD_REQUEST, "Bad request");
    Response response = mapper.toResponse(ex);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    assertEquals("Bad request", response.getEntity().toString());
  }

}
