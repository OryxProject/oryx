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

package com.cloudera.oryx.lambda.serving;

import javax.servlet.RequestDispatcher;
import javax.ws.rs.core.Response;

import org.junit.Test;
import org.springframework.mock.web.MockHttpServletRequest;

import com.cloudera.oryx.common.OryxTest;

public final class ErrorResourceTest extends OryxTest {

  @Test
  public void testError() {
    MockHttpServletRequest mockRequest = new MockHttpServletRequest();
    mockRequest.setAttribute(RequestDispatcher.ERROR_STATUS_CODE, 500);
    mockRequest.setAttribute(RequestDispatcher.ERROR_REQUEST_URI, "http://foo/bar");
    mockRequest.setAttribute(RequestDispatcher.ERROR_MESSAGE, "Something was wrong");
    mockRequest.setAttribute(RequestDispatcher.ERROR_EXCEPTION, new IllegalStateException());
    Response response  = new ErrorResource().errorHTML(mockRequest);
    assertEquals(500, response.getStatus());
    String responseBody = response.getEntity().toString();
    assertTrue(responseBody.contains("500"));
    assertTrue(responseBody.contains("http://foo/bar"));
    assertTrue(responseBody.contains("Something was wrong"));
    assertTrue(responseBody.contains("IllegalStateException"));
  }

}
