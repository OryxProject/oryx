/*
* Copyright (c) 2014, Cloudera and Intel, Inc. All Rights Reserved.
*
* Cloudera, Inc. licenses this file to you under the Apache License,
* Version 2.0 (the "License"). You may not use this file except in
* compliance with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
* CONDITIONS OF ANY KIND, either express or implied. See the License for
* the specific language governing permissions and limitations under the
* License.
*/

package com.cloudera.oryx.ml.serving.als;

import javax.ws.rs.core.Response;

import org.junit.Assert;
import org.junit.Test;

public final class ReadyTest extends AbstractALSServingTest {

  @Test
  public void testGet() {
    Response response = target("/ready").request().get();
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
  }

  @Test
  public void testHead() {
    Response response = target("/ready").request().head();
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
  }
}