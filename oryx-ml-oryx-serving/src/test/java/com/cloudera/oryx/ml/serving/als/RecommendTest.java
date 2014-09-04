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

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.oryx.ml.serving.IDValue;

public final class RecommendTest extends AbstractALSServingTest {

  @Test
  public void testRecommend() {
    List<IDValue> recs = target("/recommend/U0").request().get(LIST_ID_VALUE_TYPE);
    Assert.assertNotNull(recs);
    Assert.assertEquals(6, recs.size());
    for (int i = 1; i < recs.size(); i++) {
      Assert.assertTrue(recs.get(i).getValue() <= recs.get(i-1).getValue());
    }
    Assert.assertEquals("I1", recs.get(0).getID());
    Assert.assertEquals(0.465396924146558, recs.get(0).getValue(), FLOAT_EPSILON);
  }

  @Test
  public void testHowMany() {
    // There are 9 recs available
    testHowMany(10, 9);
    testHowMany(9, 9);
    testHowMany(5, 5);
  }

  private void testHowMany(int howMany, int expectedSize) {
    List<?> recommendations = target("/recommend/U5")
        .queryParam("howMany",Integer.toString(howMany)).queryParam("considerKnownItems", "true")
        .request().get(LIST_ID_VALUE_TYPE);
    Assert.assertEquals(expectedSize, recommendations.size());
  }

  @Test
  public void testOffset() {
    // There are only 3 recs available
    testOffset(2, 1, 2);
    testOffset(3, 1, 2);
    testOffset(1, 1, 1);
    testOffset(3, 3, 0);
  }

  private void testOffset(int howMany, int offset, int expectedSize) {
    List<?> recommendations = target("/recommend/U6")
        .queryParam("howMany", Integer.toString(howMany))
        .queryParam("considerKnownItems", "false")
        .queryParam("offset", Integer.toString(offset))
        .request().get(LIST_ID_VALUE_TYPE);
    Assert.assertEquals(expectedSize, recommendations.size());
  }

  @Test
  public void testConsiderKnownItems() {
    List<IDValue> normal = target("/recommend/U4")
        .request().get(LIST_ID_VALUE_TYPE);
    Assert.assertEquals(3, normal.size());
    Assert.assertEquals("I2", normal.get(0).getID());
    Assert.assertEquals(0.141347957620267, normal.get(0).getValue(), FLOAT_EPSILON);

    List<IDValue> withConsider = target("/recommend/U4")
        .queryParam("considerKnownItems", "true")
        .request().get(LIST_ID_VALUE_TYPE);
    Assert.assertEquals(9, withConsider.size());
    Assert.assertEquals("I7", withConsider.get(0).getID());
    Assert.assertEquals(2.00474569593095, withConsider.get(0).getValue(), FLOAT_EPSILON);
  }

  @Test(expected = BadRequestException.class)
  public void testNoArg() {
    target("/recommend").request().get(String.class);
  }

}
