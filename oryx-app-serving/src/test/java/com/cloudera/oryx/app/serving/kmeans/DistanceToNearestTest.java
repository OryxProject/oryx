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

package com.cloudera.oryx.app.serving.kmeans;

import javax.ws.rs.core.MediaType;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

public final class DistanceToNearestTest extends AbstractKMeansServingTest {

  @Test
  public void testDistance() {
    String prediction = target("/distanceToNearest/1,0").request()
        .accept(MediaType.APPLICATION_JSON_TYPE).get(String.class);
    Assert.assertEquals(0.0, Double.parseDouble(prediction), OryxTest.DOUBLE_EPSILON);
  }

  @Test
  public void testDistance2() {
    String prediction = target("/distanceToNearest/10,-1.0").request().get(String.class);
    Assert.assertEquals(64.0, Double.parseDouble(prediction), OryxTest.DOUBLE_EPSILON);
  }

}
