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
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public final class EstimateTest extends AbstractALSServingTest {

  @Override
  protected Class<?> getResourceClass() {
    return Estimate.class;
  }

  @Test
  @Ignore
  public void test() {
    GenericType<List<Double>> genericList = new GenericType<List<Double>>() {
    };
    List<Double> items = target("estimate").path("1").path("1/2/3/4/5").request()
        .accept(MediaType.APPLICATION_JSON_TYPE).get(genericList);
    Assert.assertTrue(items.size() > 0);
  }
}
