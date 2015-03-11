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

import javax.ws.rs.core.MediaType;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

public final class FeatureImportanceTest extends AbstractRDFServingTest {

  @Test
  public void testImportance() {
    double importance0 = target("/feature/importance/0").request()
        .accept(MediaType.APPLICATION_JSON_TYPE).get(Double.class);
    Assert.assertEquals(0.1, importance0, OryxTest.DOUBLE_EPSILON);
    double importance1 = target("/feature/importance/1").request()
        .accept(MediaType.APPLICATION_JSON_TYPE).get(Double.class);
    Assert.assertEquals(0.3, importance1, OryxTest.DOUBLE_EPSILON);
  }

  @Test
  public void testAllImportance() {
    @SuppressWarnings("unchecked")
    List<Double> importances = (List<Double>) target("/feature/importance").request()
        .accept(MediaType.APPLICATION_JSON_TYPE).get(List.class);
    Assert.assertEquals(Arrays.asList(0.1, 0.3), importances);
  }

}
