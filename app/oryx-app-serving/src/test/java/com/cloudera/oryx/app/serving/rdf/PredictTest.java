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

package com.cloudera.oryx.app.serving.rdf;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;


public final class PredictTest extends AbstractRDFServingTest {

  private static final String PREDICT_DATA = "A,-5,\nB,0,\n";
  private static final String[][] EXPECTED_TOPIC = {
      {"A", "-5"}, {"B", "0"}
  };

  @Test
  public void testPredict() {
    String prediction = target("/predict/B,0,").request()
        .accept(MediaType.APPLICATION_JSON_TYPE).get(String.class);
    double expectedValue = (10.0 + 2 * 1000.0) / 3;
    Assert.assertEquals(expectedValue, Double.parseDouble(prediction), OryxTest.DOUBLE_EPSILON);
  }

  @Test
  public void testPredict2() {
    String prediction = target("/predict/A,-5,").request().get(String.class);
    double expectedValue = (1.0 + 2.0 * 100.0) / 3.0;
    Assert.assertEquals(expectedValue, Double.parseDouble(prediction), OryxTest.DOUBLE_EPSILON);
  }

  @Test
  public void testFormPredict() throws Exception {
    checkResponse(getFormPostResponse(PREDICT_DATA, "/predict", null, null));
  }

  @Test
  public void testPredictPost() {
    String prediction = target("/predict").request().post(Entity.text("A,-5,\nB,0,"))
        .readEntity(String.class);
    double expectedValue1 = (1.0 + 2.0 * 100.0) / 3.0;
    double expectedValue2 = (10.0 + 2 * 1000.0) / 3;
    Assert.assertEquals(expectedValue1 + "\n" + expectedValue2 + '\n', prediction);
  }

  private static void checkResponse(Response response) {
    checkResponse(response, Response.Status.OK, EXPECTED_TOPIC);
  }

}
