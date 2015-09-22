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

package com.cloudera.oryx.app.serving.als;

import java.util.List;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.MediaType;

import org.junit.Assert;
import org.junit.Test;

public final class EstimateTest extends AbstractALSServingTest {

  @Test
  public void testEstimate() {
    List<Double> items = target("estimate/U0/I0/I1/I2").request()
        .accept(MediaType.APPLICATION_JSON_TYPE).get(LIST_DOUBLE_TYPE);
    Assert.assertEquals(3, items.size());
    Assert.assertEquals(0.38761318f, items.get(0), FLOAT_EPSILON);
    Assert.assertEquals(0.4653969f, items.get(1), FLOAT_EPSILON);
    Assert.assertEquals(-0.0801478f, items.get(2), FLOAT_EPSILON);
  }

  @Test
  public void testEstimateCSV() {
    String response = target("estimate/U0/I0/I1/I2").request().get(String.class);
    testCSVScores(3, response);
  }

  @Test(expected = NotFoundException.class)
  public void testBadRequest() {
    target("/estimate/Z").request().get(String.class);
  }

  @Test
  public void testZeroEstimate() {
    List<Double> items = target("/estimate/U0/I10").request()
        .accept(MediaType.APPLICATION_JSON_TYPE).get(LIST_DOUBLE_TYPE);
    Assert.assertEquals(1, items.size());
    Assert.assertEquals(0.0f, items.get(0), FLOAT_EPSILON);
  }

}
