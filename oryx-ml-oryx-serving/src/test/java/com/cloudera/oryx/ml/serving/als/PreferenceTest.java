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

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

import org.junit.Assert;
import org.junit.Test;

public final class PreferenceTest extends AbstractALSServingTest {

  @Test
  public void testPost() {
    Response response = target("/pref/U1/I2").request().post(Entity.text("1"));
    checkResponse(response, "U1", "I2", "1");
  }

  @Test
  public void testDelete() {
    Response response = target("/pref/U1/I2").request().delete();
    checkResponse(response, "U1", "I2", "");
  }

  private static void checkResponse(Response response, String user, String item, String score) {
    Assert.assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    /*
    List<Pair<String,String>> data = MockQueueProducer.getData();
    Assert.assertEquals(1, data.size());
    Pair<String,String> expected = data.get(0);
    Assert.assertNull(expected.getFirst());
    Assert.assertEquals(user + "," + item + "," + score, expected.getSecond());
     */
  }

}
