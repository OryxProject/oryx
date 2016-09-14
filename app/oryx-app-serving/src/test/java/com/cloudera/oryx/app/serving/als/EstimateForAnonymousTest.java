/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.oryx.app.serving.als;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.MediaType;

import org.junit.Assert;
import org.junit.Test;

public final class EstimateForAnonymousTest extends AbstractALSServingTest {

  @Test
  public void testEstimateForAnonymous() {
    Double item = target("/estimateForAnonymous/I7/I4=1.0/I5=2.0").request()
        .accept(MediaType.APPLICATION_JSON_TYPE).get(Double.class);
    Assert.assertEquals(0.3596476256862573, item, DOUBLE_EPSILON);
  }

  @Test
  public void testEstimateForAnonymousWithUnknown() {
    Double item = target("/estimateForAnonymous/I3/foo/I4=1.0/I5=2.0").request()
        .accept(MediaType.APPLICATION_JSON_TYPE).get(Double.class);
    Assert.assertEquals(-0.0670749299183302, item, DOUBLE_EPSILON);
  }

  @Test
  public void testEstimateForAnonymousWithAllUnknown() {
    Double item = target("/estimateForAnonymous/I3/foo").request()
        .accept(MediaType.APPLICATION_JSON_TYPE).get(Double.class);
    Assert.assertEquals(0.0, item, DOUBLE_EPSILON);
  }

  @Test
  public void testEstimateForAnonymousCSV() {
    String response = target("/estimateForAnonymous/I3/I4=1.0/I5=2.0").request()
        .get(String.class);
    Assert.assertEquals(-0.0670749299183302, Double.parseDouble(response), DOUBLE_EPSILON);
  }

  @Test(expected = NotFoundException.class)
  public void testBadRequest() {
    target("/estimateForAnonymous").request().get(String.class);
  }

  @Test(expected = NotFoundException.class)
  public void testUnknownUser() {
    target("/estimateForAnonymous/foo").request().get(String.class);
  }

}
