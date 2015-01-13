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

import javax.ws.rs.core.MediaType;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.oryx.app.serving.IDCount;

public final class MostActiveUsersTest extends AbstractALSServingTest {

  @Test
  public void testMostPopular() {
    List<IDCount> top = target("/mostActiveUsers").request()
        .accept(MediaType.APPLICATION_JSON_TYPE).get(LIST_ID_COUNT_TYPE);
    Assert.assertEquals(7, top.size());
    for (int i = 0; i < top.size(); i++) {
      int thisCount = top.get(i).getValue();
      Assert.assertTrue(thisCount >= 1);
      if (i > 0) {
        Assert.assertTrue(top.get(i - 1).getValue() >= thisCount);
      }
    }
    Assert.assertEquals(7, top.get(0).getValue());
    Assert.assertEquals(6, top.get(1).getValue());
  }

  @Test
  public void testMostPopularCSV() {
    String response = target("/mostActiveUsers").request().get(String.class);
    testCSVTopByCount(7, response);
  }

}
