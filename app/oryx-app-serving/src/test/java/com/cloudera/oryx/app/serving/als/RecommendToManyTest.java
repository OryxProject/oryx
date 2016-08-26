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

package com.cloudera.oryx.app.serving.als;

import java.util.List;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.MediaType;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.oryx.app.serving.IDValue;

public final class RecommendToManyTest extends AbstractALSServingTest {

  @Test
  public void testRecommendToMany() {
    List<IDValue> recs = target("/recommendToMany/U0/U2").request()
        .accept(MediaType.APPLICATION_JSON_TYPE).get(LIST_ID_VALUE_TYPE);
    testTopByValue(2, recs, false);
    Assert.assertEquals("I1", recs.get(0).getID());
    Assert.assertEquals(0.34344634f, recs.get(0).getValue(), FLOAT_EPSILON);
  }

  @Test(expected = NotFoundException.class)
  public void testNoArg() {
    target("/recommendToMany").request().get(String.class);
  }

  @Test
  public void testHowMany() {
    testHowMany("/recommendToMany/U2/U5", 10, 2);
    testHowMany("/recommendToMany/U5", 2, 2);
    testHowMany("/recommendToMany/U2", 1, 1);
  }

  @Test(expected = BadRequestException.class)
  public void testBadHowMany() {
    testHowMany("/recommendToMany/U5", -1, 0);
  }

  @Test
  public void testOffset() {
    testOffset("/recommendToMany/U0/U6", 5, 0, 1);
    testOffset("/recommendToMany/U0/U6", 3, 0, 1);
    testOffset("/recommendToMany/U6", 1, 1, 1);
    testOffset("/recommendToMany/U6", 3, 3, 0);
  }

  @Test(expected = BadRequestException.class)
  public void testTooMany() {
    testHowMany("/recommendToMany/U5", 100001, 0);
  }

  @Test(expected = BadRequestException.class)
  public void testBadOffset() {
    testOffset("/recommendToMany/U6", 3, -1, 0);
  }

  @Test
  public void testConsiderKnownItems() {
    List<IDValue> normal = target("/recommendToMany/U4").request()
        .accept(MediaType.APPLICATION_JSON_TYPE).get(LIST_ID_VALUE_TYPE);
    Assert.assertEquals(3, normal.size());
    Assert.assertEquals("I2", normal.get(0).getID());
    Assert.assertEquals(0.14134796f, normal.get(0).getValue(), FLOAT_EPSILON);

    List<IDValue> withConsider = target("/recommendToMany/U4")
        .queryParam("considerKnownItems", "true").request()
        .accept(MediaType.APPLICATION_JSON_TYPE).get(LIST_ID_VALUE_TYPE);
    Assert.assertEquals(9, withConsider.size());
    Assert.assertEquals("I7", withConsider.get(0).getID());
    Assert.assertEquals(2.0047457f, withConsider.get(0).getValue(), FLOAT_EPSILON);
  }

  @Test
  public void testRecommendToManyCSV() {
    String response = target("/recommendToMany/U0/U2").request().get(String.class);
    testCSVTopByScore(2, response);
  }

  @Test
  public void testRescorer() {
    List<IDValue> recs = target("/recommendToMany/U0/U2")
        .queryParam("rescorerParams", "foo").request()
        .accept(MediaType.APPLICATION_JSON_TYPE).get(LIST_ID_VALUE_TYPE);
    testTopByValue(2, recs, false);
    Assert.assertEquals("I1", recs.get(0).getID());
    Assert.assertEquals(2.0f * 0.34344634f, recs.get(0).getValue(), FLOAT_EPSILON);
  }

}
