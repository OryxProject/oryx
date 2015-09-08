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

package com.cloudera.oryx.app.serving.rdf;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.lambda.serving.MockTopicProducer;

public final class TrainTest extends AbstractRDFServingTest {

  static final String TRAIN_DATA = "B,0,20\nB,-4,30\nA,0,40\nA,-4,50";
  private static final String[][] EXPECTED_TOPIC = {
      {"B", "0", "20"},
      {"B", "-4", "30"},
      {"A", "0", "40"},
      {"A", "-4", "50"},
  };

  @Test
  public void testSimpleTrain() {
    checkResponse(target("/train").request().post(Entity.text(TRAIN_DATA)));
  }

  @Test
  public void testFormTrain() throws Exception {
    checkResponse(getFormPostResponse(TRAIN_DATA, "/train", null, null));
  }

  @Test
  public void testURITrain() throws Exception {
    Response response = target("/train/" + TRAIN_DATA.split("\n")[0])
        .request().post(Entity.text(""));
    Assert.assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    List<Pair<String,String>> data = MockTopicProducer.getData();
    Assert.assertEquals(1, data.size());
    Assert.assertArrayEquals(EXPECTED_TOPIC[0], data.get(0).getSecond().split(","));
  }

  private static void checkResponse(Response response) {
    checkResponse(response, Response.Status.NO_CONTENT, EXPECTED_TOPIC);
  }

}
