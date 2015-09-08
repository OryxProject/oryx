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

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.lambda.serving.MockTopicProducer;

public final class AddTest extends AbstractKMeansServingTest {

  static final String ADD_DATA = "1.0,0.0,20.0\n1.0,-4.0,30.0\n0.0,0.0,40.0\n0.0,-4.0,50.0";
  private static final String[][] EXPECTED_TOPIC = {
      {"1.0", "0.0", "20.0"},
      {"1.0", "-4.0", "30.0"},
      {"0.0", "0.0", "40.0"},
      {"0.0", "-4.0", "50.0"},
  };

  @Test
  public void testSimpleAdd() {
    checkResponse(target("/add").request().post(Entity.text(ADD_DATA)));
  }

  @Test
  public void testFormAdd() throws Exception {
    checkResponse(getFormPostResponse(ADD_DATA, "/add", null, null));
  }

  @Test
  public void testURIAdd() throws Exception {
    Response response = target("/add/" + ADD_DATA.split("\n")[0]).request().post(Entity.text(""));
    Assert.assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    List<Pair<String,String>> data = MockTopicProducer.getData();
    Assert.assertEquals(1, data.size());
    Assert.assertArrayEquals(EXPECTED_TOPIC[0], data.get(0).getSecond().split(","));
  }

  private static void checkResponse(Response response) {
    checkResponse(response, Response.Status.NO_CONTENT, EXPECTED_TOPIC);
  }

}
