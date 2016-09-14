/*
 * Copyright (c) 2014, Cloudera, Inc. and Intel Corp. All Rights Reserved.
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

import com.cloudera.oryx.app.serving.IDValue;
import org.junit.Assert;
import org.junit.Test;

public final class SimilarityTest extends AbstractALSServingTest {

  @Test(expected = NotFoundException.class)
  public void testNoArg() {
    target("/similarity").request().get(String.class);
  }

  @Test
  public void testSimilarItems() {
    List<IDValue> recs = target("similarity/I0/I4/I6").request()
        .accept(MediaType.APPLICATION_JSON_TYPE).get(LIST_ID_VALUE_TYPE);
    testTopByValue(6, recs, false);
    Assert.assertEquals("I5", recs.get(1).getID());
    Assert.assertEquals(0.9125432970065859, recs.get(2).getValue(), DOUBLE_EPSILON);
  }

  @Test
  public void testHowMany() {
    testHowMany("/similarity/I0/I2/I4", 10, 6);
    testHowMany("/similarity/I0/I2/I4", 9, 6);
    testHowMany("/similarity/I0/I2/I4", 5, 5);
  }

  @Test
  public void testOffset() {
    testOffset("/similarity/I0/I2/I6", 2, 1, 2);
    testOffset("/similarity/I0/I2/I6", 3, 1, 3);
    testOffset("/similarity/I0/I2/I6", 1, 1, 1);
    testOffset("/similarity/I0/I2/I6", 3, 3, 3);
  }

  @Test
  public void testSimilarityCSV() {
    String response = target("/similarity/I0/I4/I6").request().get(String.class);
    testCSVTopByScore(6, response);
  }

  @Test(expected = BadRequestException.class)
  public void testBadHowMany() {
    testHowMany("/similarity/I0/I2/I4", -1, 0);
  }

  @Test(expected = BadRequestException.class)
  public void testBadOffset() {
    testOffset("/similarity/I0/I2/I6", 3, -1, 0);
  }

  @Test(expected = BadRequestException.class)
  public void testTooMany() {
    testHowMany("/similarity/I0/I2/I4", 100001, 0);
  }

  @Test
  public void testRescorer() {
    List<IDValue> recs = target("similarity/I0/I4/I6")
        .queryParam("rescorerParams", "foo").request()
        .accept(MediaType.APPLICATION_JSON_TYPE).get(LIST_ID_VALUE_TYPE);
    testTopByValue(4, recs, false);
    Assert.assertEquals("I5", recs.get(1).getID());
    Assert.assertEquals(2.0 * 0.9125432970065859, recs.get(2).getValue(), DOUBLE_EPSILON);
  }

}
