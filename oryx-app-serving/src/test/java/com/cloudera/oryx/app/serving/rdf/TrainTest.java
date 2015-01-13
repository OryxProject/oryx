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
import org.junit.Before;
import org.junit.Test;

import com.cloudera.oryx.app.serving.MockTopicProducer;
import com.cloudera.oryx.common.collection.Pair;

public final class TrainTest extends AbstractRDFServingTest {

  private static final String TRAIN_DATA = "B,0,20\nB,-4,30\nA,0,40\nA,-4,50";
  private static final String[][] EXPECTED_TOPIC = {
      {"B", "0", "20"},
      {"B", "-4", "30"},
      {"A", "0", "40"},
      {"A", "-4", "50"},
  };


  @Before
  public void clearProducerData() {
    MockTopicProducer.getData().clear();
  }

  @Test
  public void testSimpleTrain() {
    checkResponse(target("/train").request().post(Entity.text(TRAIN_DATA)));
  }

  @Test
  public void testFormTrain() throws Exception {
    checkResponse(getFormPostResponse(TRAIN_DATA, "/train", null, null));
  }

  private static void checkResponse(Response response) {
    Assert.assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    List<Pair<String,String>> data = MockTopicProducer.getData();
    for (int i = 0; i < data.size(); i++) {
      Pair<String,String> actual = data.get(i);
      Assert.assertNull(actual.getFirst());
      String[] tokens = actual.getSecond().split(",");
      Assert.assertEquals(EXPECTED_TOPIC[i][0], tokens[0]);
      Assert.assertEquals(EXPECTED_TOPIC[i][1], tokens[1]);
      Assert.assertEquals(EXPECTED_TOPIC[i][2], tokens[2]);
    }
  }

}
