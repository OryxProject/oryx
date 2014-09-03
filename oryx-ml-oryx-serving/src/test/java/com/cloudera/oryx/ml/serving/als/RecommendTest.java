/*
 * Copyright (c) 2014, Cloudera and Intel, Inc. All Rights Reserved.
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
import javax.ws.rs.BadRequestException;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public final class RecommendTest extends AbstractALSServingTest {

  @Test
  public void testRecommend() throws Exception {
    String response = target("/recommend/Z").request().get(String.class);
    Assert.assertNotNull(response);
    List<?> recommendations = new ObjectMapper().readValue(response, List.class);
    Assert.assertEquals(3, recommendations.size());
    // TODO
  }

  @Test(expected = BadRequestException.class)
  public void testNoArg() {
    target("/recommend").request().get(String.class);
  }

}
