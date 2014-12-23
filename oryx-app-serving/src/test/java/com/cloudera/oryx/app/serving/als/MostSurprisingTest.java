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
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.MediaType;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.oryx.app.serving.IDValue;

public final class MostSurprisingTest extends AbstractALSServingTest {

  @Test(expected = NotFoundException.class)
  public void testNoArg() {
    target("/mostSurprising").request().get(String.class);
  }

  @Test
  public void testMostSurprising() {
    List<IDValue> recs = target("/mostSurprising/U0").request()
        .accept(MediaType.APPLICATION_JSON_TYPE).get(LIST_ID_VALUE_TYPE);
    testTopByValue(3, recs, true);
    Assert.assertEquals("I0", recs.get(0).getID());
    Assert.assertEquals(0.38761317555864894, recs.get(0).getValue(), DOUBLE_EPSILON);
  }

  @Test
  public void testMostSurprisingCSV() {
    String response = target("/mostSurprising/U0").request().get(String.class);
    testCSVLeastByScore(3, response);
  }

  @Test
  public void testHowMany() {
    testHowMany("/mostSurprising/U4", 10, 6);
    testHowMany("/mostSurprising/U4", 9, 6);
    testHowMany("/mostSurprising/U4", 5, 5);
  }

  @Test
  public void testOffset() {
    testOffset("/mostSurprising/U4", 10, 0, 6);
    testOffset("/mostSurprising/U4", 9, 3, 3);
    testOffset("/mostSurprising/U4", 5, 6, 0);
  }

}
