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
    List<Double> items = target("estimate").path("Z").path("A/B/C").request()
        .accept(MediaType.APPLICATION_JSON_TYPE).get(LIST_DOUBLE_TYPE);
    Assert.assertEquals(3, items.size());
    Assert.assertEquals(11.0, items.get(0), DOUBLE_EPSILON);
  }

  @Test
  public void testBadRequest() {
    Response response = target("estimate").path("Z").request().accept(MediaType.APPLICATION_JSON_TYPE).get();
    Assert.assertEquals(404, response.getStatus());
    response = target("estimate").path("Z").path("").request().accept(MediaType.APPLICATION_JSON_TYPE).get();
    Assert.assertEquals(404, response.getStatus());
  }
}
