/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
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
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.junit.Assert;
import org.junit.Test;

public final class EstimateTest extends AbstractALSServingTest {

  @Test
  public void test() {
    List<Double> items = target("estimate").path("U0").path("I0/I1/I2").request()
        .accept(MediaType.APPLICATION_JSON_TYPE).get(LIST_DOUBLE_TYPE);
    Assert.assertEquals(3, items.size());
    Assert.assertEquals(0.387613186054306, items.get(0), FLOAT_EPSILON);
    Assert.assertEquals(0.465396924146558, items.get(1), FLOAT_EPSILON);
    Assert.assertEquals(-0.0801478014434228, items.get(2), FLOAT_EPSILON);
  }

  @Test
  public void testBadRequest() {
    Response response = target("estimate").path("Z").request()
        .accept(MediaType.APPLICATION_JSON_TYPE).get();
    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    response = target("estimate").path("Z").path("").request()
        .accept(MediaType.APPLICATION_JSON_TYPE).get();
    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }
}
