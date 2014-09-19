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

package com.cloudera.oryx.ml.serving.als;

import java.util.List;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.ml.serving.CSVMessageBodyWriter;

public final class PreferenceTest extends AbstractALSServingTest {

  private static final String PREFERENCE_DATA = "2.5f\n";

  @Before
  public void clearProducerData() {
    MockQueueProducer.getData().clear();
  }

  @Test
  public void testPostJson() {
    Response response = target("/pref/U1/I1").request()
        .post(Entity.entity(PREFERENCE_DATA, MediaType.APPLICATION_JSON));
    checkResponse(response, "U1,I1,2.5");
  }

  @Test
  public void testPostWithEmptyItemValue() {
    Response response = target("/pref/U2/I2").request().post(Entity.text(""));
    checkResponse(response, "U2,I2,1.0");
  }

  @Test
  public void testPostWithBadItemValue() {
    Response response = target("/pref/U2/I2").request().post(Entity.text("aBc!"));
    Assert.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }

  @Test
  public void testPostCSV() {
    Response response = target("/pref/U1/I1").request()
        .post(Entity.entity(PREFERENCE_DATA, CSVMessageBodyWriter.TEXT_CSV_TYPE));
    checkResponse(response, "U1,I1,2.5");
  }

  @Test
  public void testDelete() {
    Response response = target("/pref/U1/I2").request().delete();
    checkResponse(response, "U1,I2");
  }

  private static void checkResponse(Response response, String expectedOutput) {
    Assert.assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    List<Pair<String,String>> data = MockQueueProducer.getData();
    for (Pair<String, String> expected : data) {
      Assert.assertNull(expected.getFirst());
      Assert.assertEquals(expectedOutput, expected.getSecond());
    }
  }

}
