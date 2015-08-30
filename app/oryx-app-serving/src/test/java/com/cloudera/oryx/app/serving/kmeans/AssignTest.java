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

package com.cloudera.oryx.app.serving.kmeans;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.junit.Assert;
import org.junit.Test;

public final class AssignTest extends AbstractKMeansServingTest {

  private static final String ASSIGN_DATA = "-1.5,0.5\n1.0,0.0\n-1.0,0.0\n0.5,2.0\n";
  private static final String[][] EXPECTED_TOPIC = {
      {"-1.5", "-0.5"},
      {"1.0", "0.0"},
      {"-1.0", "0.0"},
      {"0.5", "2.0"}
  };

  @Test
  public void testAssign() {
    String prediction = target("/assign/1,0").request()
        .accept(MediaType.APPLICATION_JSON_TYPE).get(String.class);
    Assert.assertEquals(2, Integer.parseInt(prediction));
  }

  @Test
  public void testAssign2() {
    String prediction = target("/assign/10,-1.0").request().get(String.class);
    Assert.assertEquals(3, Integer.parseInt(prediction));
  }

  @Test
  public void testAssignPost() {
    String prediction = target("/assign").request().post(Entity.text("-1.5,0.5\n-1,0"))
        .readEntity(String.class);
    Assert.assertEquals("4\n4\n", prediction);
  }

  @Test
  public void testFormAssign() throws Exception {
    checkResponse(getFormPostResponse(ASSIGN_DATA, "/assign", null, null));
  }

  private static void checkResponse(Response response) {
    checkResponse(response, Response.Status.OK, EXPECTED_TOPIC);
  }

}
